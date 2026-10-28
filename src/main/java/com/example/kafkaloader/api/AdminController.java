package com.example.kafkaloader.api;

import com.example.kafkaloader.cluster.ClusterDefinitionProvider;
import com.example.kafkaloader.config.FtpProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.config.KafkaSecurityAssembler;
import com.example.kafkaloader.file.DataSchema;
import com.example.kafkaloader.file.FileStagingManager;
import com.example.kafkaloader.file.FileStats;
import com.example.kafkaloader.file.FileUploadService;
import com.example.kafkaloader.file.UploadResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 运维接口：手动封盘、手动上传、查看管道状态与字段定义。
 * <p>
 * 节点配置在 application.yml 里，改完需重启，因此这里不提供「刷新节点」入口。
 */
@RestController
@RequestMapping("/api/admin")
@Tag(name = "运维接口", description = "手动封盘/上传分片文件、查看管道状态与字段定义")
public class AdminController {

    private final FileStagingManager stagingManager;
    private final FileUploadService uploadService;
    private final ClusterDefinitionProvider clusterProvider;
    private final KafkaClusterProperties clusterProperties;
    private final FtpProperties ftpProperties;
    private final KafkaSecurityAssembler securityAssembler;

    public AdminController(FileStagingManager stagingManager,
                           FileUploadService uploadService,
                           ClusterDefinitionProvider clusterProvider,
                           KafkaClusterProperties clusterProperties,
                           FtpProperties ftpProperties,
                           KafkaSecurityAssembler securityAssembler) {
        this.stagingManager = stagingManager;
        this.uploadService = uploadService;
        this.clusterProvider = clusterProvider;
        this.clusterProperties = clusterProperties;
        this.ftpProperties = ftpProperties;
        this.securityAssembler = securityAssembler;
    }

    @PostMapping("/files/flush")
    @Operation(summary = "立即封盘当前分片",
            description = "把未写满的分片立刻封盘并转入待上传队列，用于低峰期或停机前手动推进")
    public Map<String, Object> flushFiles() {
        int sealed = stagingManager.flushAll();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sealedFiles", sealed);
        result.put("pendingFiles", stagingManager.stats().getPendingFiles());
        result.put("message", sealed > 0 ? "已封盘 " + sealed + " 个分片" : "当前没有待封盘数据");
        return result;
    }

    @PostMapping("/files/upload")
    @Operation(summary = "立即上传待处理文件",
            description = "先本地备份再上传 FTP，与定时任务共用互斥锁，不会并发执行")
    public UploadResult uploadFiles() {
        return uploadService.uploadPending();
    }

    @GetMapping("/files/stats")
    @Operation(summary = "文件管道统计", description = "正在写入、待上传、已备份、隔离区文件数与磁盘剩余空间")
    public FileStats fileStats() {
        return stagingManager.stats();
    }

    @GetMapping("/schema")
    @Operation(summary = "文件字段顺序",
            description = "生成文件每行的列顺序，与《数据表字段清单》完全一致，下游按位置装载")
    public Map<String, Object> schema() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("fieldCount", DataSchema.SIZE);
        result.put("delimiter", "\\t");
        result.put("fileExtension", stagingManager.getFileExtension());
        List<Map<String, Object>> fields = new ArrayList<>(DataSchema.SIZE);
        for (int i = 0; i < DataSchema.SIZE; i++) {
            DataSchema.Field field = DataSchema.FIELDS.get(i);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", i + 1);
            item.put("name", field.getName());
            item.put("type", field.getType().name());
            item.put("description", field.getDescription());
            fields.add(item);
        }
        result.put("fields", fields);
        return result;
    }

    @GetMapping("/status")
    @Operation(summary = "服务状态总览")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("time", LocalDateTime.now().toString());
        status.put("clusterName", clusterProperties.getName());
        status.put("topic", clusterProperties.getTopic());
        // 生成文件的命名前缀与规则，便于运维在 FTP 侧按名核对
        status.put("systemName", stagingManager.getSystemName());
        status.put("fileNamePattern", stagingManager.getSystemName() + "_<yyyyMMddHHmmssSSS>"
                + stagingManager.getFileExtension());
        status.put("bootstrapServers", clusterProvider.getBootstrapServers());
        status.put("nodeCount", clusterProvider.get().getNodes().size());
        // 节点来源固定为配置文件；改节点需改 yml 后重启
        status.put("nodeSource", "application.yml (kafka.cluster.bootstrap-servers)");
        // 连接安全摘要（协议/机制/principal/证书文件名），不含任何密码
        status.put("kafkaSecurity", securityAssembler.getSummary());
        status.put("ftpEnabled", ftpProperties.isEnabled());
        status.put("ftpHost", ftpProperties.getHost() + ":" + ftpProperties.getPort());
        status.put("ftpRemoteDir", ftpProperties.getRemoteDir());
        status.put("uploadRunning", uploadService.isRunning());
        status.put("fieldCount", DataSchema.SIZE);
        status.put("rejectedRecords", stagingManager.getRejectedRecords());
        // 进程启动以来写入主文件的记录条数（一行一条）
        status.put("writtenRecords", stagingManager.getWrittenRecords());
        status.put("rejectDir", stagingManager.getRejectDir().toString());
        status.put("files", stagingManager.stats());
        return status;
    }
}
