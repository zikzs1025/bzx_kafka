package com.example.kafkaloader.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

/**
 * 分片文件名分配器：统一产出 {@code 系统配置名称_时间戳<扩展名>}。
 * <p>
 * 命名规则：
 * <ul>
 *   <li>系统配置名称：{@code file.system-name}，未配置时回退 {@code kafka.cluster.name}</li>
 *   <li>时间戳：封盘时刻的 17 位毫秒时间戳（{@code yyyyMMddHHmmssSSS}），天然可排序</li>
 *   <li>扩展名：{@code file.file-extension}，默认 {@code .txt}（字段以 {@code \t} 分隔）</li>
 * </ul>
 * 并发下同一毫秒可能撞名：先查内存占位集合、再查磁盘，冲突则追加 {@code -1/-2...} 保证唯一，
 * 绝不覆盖已存在的文件（覆盖等于丢数据）。
 */
class FileNameAllocator {

    /** 毫秒级时间戳；带毫秒是为了让并发的多个分区封盘也能排到不同时刻 */
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    /** 默认扩展名：制表符分隔的纯文本 */
    static final String DEFAULT_EXTENSION = ".txt";

    /** 单毫秒内最大尝试次数，超出说明系统异常（例如同一毫秒写入上百个文件） */
    private static final int MAX_ATTEMPT = 1000;

    /** 内存占位上限，超过后清理：历史名字的磁盘文件仍在，靠 Files.exists 兜底即可 */
    private static final int RESERVED_CAPACITY = 50_000;

    private final String systemName;
    private final String extension;
    private final Set<String> reserved = new HashSet<>();

    FileNameAllocator(String systemName) {
        this(systemName, DEFAULT_EXTENSION);
    }

    FileNameAllocator(String systemName, String extension) {
        this.systemName = sanitize(systemName);
        this.extension = normalizeExtension(extension);
    }

    String getSystemName() {
        return systemName;
    }

    String getExtension() {
        return extension;
    }

    /**
     * 为即将封盘的文件分配一个在 {@code dir} 中不存在的路径（不创建文件）。
     * 返回的路径之后通过原子 move 落地，保证下游看不到半截文件。
     */
    synchronized Path allocate(Path dir) throws IOException {
        String ts = LocalDateTime.now().format(STAMP);
        for (int attempt = 0; attempt < MAX_ATTEMPT; attempt++) {
            String name = attempt == 0
                    ? systemName + "_" + ts + extension
                    : systemName + "_" + ts + "-" + attempt + extension;
            Path candidate = dir.resolve(name);
            if (reserved.contains(name) || Files.exists(candidate)) {
                continue;
            }
            if (reserved.size() >= RESERVED_CAPACITY) {
                reserved.clear();
            }
            reserved.add(name);
            return candidate;
        }
        throw new IOException("无法为目录 " + dir + " 分配唯一文件名（同一毫秒内冲突超过 "
                + MAX_ATTEMPT + " 次）");
    }

    /** 扩展名规范化：空值回退 .txt；自动补前导点；只保留字母数字 */
    static String normalizeExtension(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_EXTENSION;
        }
        String cleaned = raw.trim().replaceAll("[^A-Za-z0-9.]", "");
        if (cleaned.isEmpty()) {
            return DEFAULT_EXTENSION;
        }
        return cleaned.startsWith(".") ? cleaned : "." + cleaned;
    }

    /**
     * 文件名合法化：保留字母、数字、中文、点、下划线、连字符，其余替换为下划线，
     * 避免 Windows 非法字符与 FTP 路径转义问题。
     */
    static String sanitize(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return "kafka";
        }
        String cleaned = raw.trim().replaceAll("[^A-Za-z0-9._\\u4e00-\\u9fa5-]", "_");
        return cleaned.isEmpty() ? "kafka" : cleaned;
    }
}
