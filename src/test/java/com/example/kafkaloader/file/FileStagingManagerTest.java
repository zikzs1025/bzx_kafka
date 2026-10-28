package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileStagingManagerTest {

    private static final String SYSTEM_NAME = "DISTRIBUTE";

    /** 系统配置名称_17位毫秒时间戳.txt，冲突时追加 -N */
    private static final String NAME_PATTERN = "^" + SYSTEM_NAME + "_\\d{17}(-\\d+)?\\.txt$";

    private FileStagingManager manager;

    /** 关闭写入器句柄，否则 Windows 下临时目录无法删除 */
    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.destroy();
            manager = null;
        }
    }

    private FileStagingManager newManager(FilePipelineProperties properties) throws Exception {
        return newManager(properties, "DISTRIBUTE");
    }

    private FileStagingManager newManager(FilePipelineProperties properties, String clusterName) throws Exception {
        KafkaClusterProperties cluster = new KafkaClusterProperties();
        cluster.setName(clusterName);
        manager = new FileStagingManager(properties, cluster);
        return manager;
    }

    private FilePipelineProperties properties(Path tempDir, int recordsPerFile) {
        FilePipelineProperties properties = new FilePipelineProperties();
        properties.setStageDir(tempDir.resolve("stage").toString());
        properties.setPendingDir(tempDir.resolve("pending").toString());
        properties.setBackupDir(tempDir.resolve("backup").toString());
        properties.setFailedDir(tempDir.resolve("failed").toString());
        properties.setRejectDir(tempDir.resolve("reject").toString());
        properties.setRecordsPerFile(recordsPerFile);
        properties.setSystemName(SYSTEM_NAME);
        properties.setFsyncPerBatch(false);
        properties.setMaxPendingFiles(0);
        properties.setMinFreeDiskMb(0);
        return properties;
    }

    /** 正常报文：字段名与《数据表字段清单》一致，故意乱序并夹带清单外字段 */
    private List<ConsumerRecord<String, String>> batch(int from, int count) {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long offset = from + i;
            String payload = "{\"xm\":\"张三\",\"md_id\":\"M" + offset + "\","
                    + "\"capture_time\":" + (1760000000000L + offset) + ","
                    + "\"zjhm\":\"110101199001011234\",\"extra\":\"忽略\"}";
            list.add(new ConsumerRecord<>("T1", 0, offset, "k" + offset, payload));
        }
        return list;
    }

    /** 无法映射的报文（纯文本） */
    private List<ConsumerRecord<String, String>> unmappableBatch(int from, int count) {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long offset = from + i;
            list.add(new ConsumerRecord<>("T1", 0, offset, "k" + offset, "not-a-json-" + offset));
        }
        return list;
    }

    private List<Path> pendingFiles(Path tempDir) throws Exception {
        Path dir = tempDir.resolve("pending");
        if (!Files.isDirectory(dir)) {
            return new ArrayList<>();
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.sorted().collect(Collectors.toList());
        }
    }

    @Test
    void shouldRollNewFileEveryConfiguredRecords(@TempDir Path tempDir) throws Exception {
        FileStagingManager manager = newManager(properties(tempDir, 5));

        // 12 条 / 每文件 5 条 => 2 个封盘文件 + 1 个在写文件（2 条）
        manager.appendBatch(batch(0, 12));

        List<Path> pending = pendingFiles(tempDir);
        assertEquals(2, pending.size());
        for (Path file : pending) {
            assertEquals(5, Files.readAllLines(file, StandardCharsets.UTF_8).size());
            assertTrue(file.getFileName().toString().matches(NAME_PATTERN),
                    "文件名应为 系统配置名称_时间戳.txt: " + file.getFileName());
        }

        // 再写 3 条把剩余写满，应再封盘 1 个
        manager.appendBatch(batch(12, 3));
        assertEquals(3, pendingFiles(tempDir).size());
    }

    @Test
    void shouldSealIdleFileEvenNotFull(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 5);
        properties.setIdleFlushMs(0L);
        FileStagingManager manager = newManager(properties);

        manager.appendBatch(batch(0, 2));
        assertEquals(0, pendingFiles(tempDir).size(), "未满且未超时不应封盘");

        assertEquals(1, manager.flushIdle());
        List<Path> pending = pendingFiles(tempDir);
        assertEquals(1, pending.size());
        assertEquals(2, Files.readAllLines(pending.get(0), StandardCharsets.UTF_8).size());
        assertTrue(pending.get(0).getFileName().toString().matches(NAME_PATTERN));
    }

    @Test
    void shouldRecoverLeftoverWritingFiles(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 5);
        Path topicDir = tempDir.resolve("stage").resolve("T1");
        Files.createDirectories(topicDir);
        Files.write(topicDir.resolve("p0-99.writing"),
                "a\tb\nc\td\ne\tf\n".getBytes(StandardCharsets.UTF_8));

        FileStagingManager manager = newManager(properties);
        manager.recoverInterruptedFiles();

        List<Path> pending = pendingFiles(tempDir);
        assertEquals(1, pending.size());
        assertEquals(3, Files.readAllLines(pending.get(0), StandardCharsets.UTF_8).size());
        assertTrue(pending.get(0).getFileName().toString().matches(NAME_PATTERN));
    }

    @Test
    void shouldRejectWhenPendingBacklogExceeded(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 5);
        properties.setMaxPendingFiles(1L);
        FileStagingManager manager = newManager(properties);

        Files.createDirectories(tempDir.resolve("pending"));
        Files.write(tempDir.resolve("pending").resolve("DISTRIBUTE_20261022083000123.csv"),
                "x\n".getBytes(StandardCharsets.UTF_8));
        manager.invalidatePendingCache();

        assertThrows(BackpressureException.class, () -> manager.appendBatch(batch(0, 1)));
    }

    @Test
    void shouldFlushAllOnShutdown(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 50000);
        FileStagingManager manager = newManager(properties);

        manager.appendBatch(batch(0, 7));
        assertEquals(0, pendingFiles(tempDir).size());

        assertEquals(1, manager.flushAll());
        assertEquals(1, pendingFiles(tempDir).size());
        assertEquals(7, Files.readAllLines(pendingFiles(tempDir).get(0), StandardCharsets.UTF_8).size());
    }

    @Test
    void shouldWriteColumnsInChecklistOrder(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 1);
        FileStagingManager manager = newManager(properties);

        manager.appendBatch(batch(100, 1));

        Path file = pendingFiles(tempDir).get(0);
        assertTrue(file.getFileName().toString().endsWith(".txt"));
        String line = Files.readAllLines(file, StandardCharsets.UTF_8).get(0);
        String[] cols = line.split("\t", -1);
        assertEquals(DataSchema.SIZE, cols.length, "列数必须等于字段清单数量 31");
        assertEquals("M100", cols[0], "第 1 列 md_id");
        assertEquals("1760000000100", cols[4], "第 5 列 capture_time");
        assertEquals("110101199001011234", cols[18], "第 19 列 zjhm");
        assertEquals("张三", cols[19], "第 20 列 xm");
        assertEquals("", cols[30], "第 31 列 synchro_time 未提供应为空字符串");
    }

    /** 无法映射的报文必须进隔离区，且不能污染主数据文件 */
    @Test
    void shouldRouteUnmappablePayloadToRejectFile(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 5);
        FileStagingManager manager = newManager(properties);

        manager.appendBatch(batch(0, 1));
        manager.appendBatch(unmappableBatch(10, 2));
        assertEquals(2, manager.getRejectedRecords());
        manager.flushAll();

        List<Path> pending = pendingFiles(tempDir);
        assertEquals(1, pending.size(), "被隔离的记录不能污染主数据文件");
        assertEquals(1, Files.readAllLines(pending.get(0), StandardCharsets.UTF_8).size());

        Path rejectDir = tempDir.resolve("reject");
        List<Path> rejectFiles = listFiles(rejectDir);
        assertEquals(1, rejectFiles.size());
        assertTrue(rejectFiles.get(0).getFileName().toString().matches("^DISTRIBUTE_reject_\\d{8}\\.txt$"),
                "隔离文件命名: " + rejectFiles.get(0).getFileName());
        List<String> lines = Files.readAllLines(rejectFiles.get(0), StandardCharsets.UTF_8);
        assertEquals(3, lines.size(), "1 行表头 + 2 行隔离记录");
        assertTrue(lines.get(0).startsWith("topic\tpartition"));
        assertTrue(lines.get(1).contains("not-a-json-10"), "原始报文必须完整保留");
    }

    private List<Path> listFiles(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            return new ArrayList<>();
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.collect(Collectors.toList());
        }
    }

    /** 同一毫秒内连封多个文件时，名字必须唯一，绝不能覆盖已存在文件 */
    @Test
    void shouldGenerateUniqueFileNamesWithinSameMillisecond(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 1);
        FileStagingManager manager = newManager(properties);

        manager.appendBatch(batch(0, 40));

        List<Path> pending = pendingFiles(tempDir);
        assertEquals(40, pending.size(), "每文件 1 条，40 条应产生 40 个文件");
        Set<String> names = new HashSet<>();
        for (Path file : pending) {
            String name = file.getFileName().toString();
            assertTrue(names.add(name), "文件名重复会覆盖数据: " + name);
            assertTrue(name.matches(NAME_PATTERN), "命名不符合规则: " + name);
        }
    }

    /** 未配置 file.system-name 时回退集群标识，文件名仍有业务含义 */
    @Test
    void shouldFallbackToClusterNameWhenSystemNameBlank(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 1);
        properties.setSystemName("  ");
        FileStagingManager manager = newManager(properties, "KAFKA_PROD");

        manager.appendBatch(batch(0, 1));

        String name = pendingFiles(tempDir).get(0).getFileName().toString();
        assertTrue(name.matches("^KAFKA_PROD_\\d{17}(-\\d+)?\\.txt$"), "实际文件名: " + name);
        assertEquals("KAFKA_PROD", manager.getSystemName());
    }

    /** 系统配置名称中的路径/非法字符要被清理，避免写到目录外或 FTP 失败 */
    @Test
    void shouldSanitizeIllegalCharactersInSystemName() {
        assertEquals("A_B_C", FileNameAllocator.sanitize("A/B C"));
        assertEquals("系统_配置", FileNameAllocator.sanitize("系统 配置"));
        assertEquals("kafka", FileNameAllocator.sanitize(null));
        assertEquals("kafka", FileNameAllocator.sanitize("   "));
    }
}
