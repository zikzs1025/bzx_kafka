package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Kafka 集群配置：全部写在 application.yml 里，不依赖数据库，也不读外部文件。
 * <p>
 * 节点变更后需重启进程生效（配置文件在启动时一次性绑定）。
 */
@ConfigurationProperties(prefix = "kafka.cluster")
public class KafkaClusterProperties {

    /** 集群标识，仅用于日志与运维展示，不参与连接 */
    private String name = "default";

    /** 消费主题。变更需重启 */
    private String topic;

    /** 节点省略端口时的兜底端口 */
    private int defaultPort = 9092;

    /**
     * broker 地址列表。yml 里两种写法都支持：
     * <pre>
     * bootstrap-servers: 10.0.0.1:9092,10.0.0.2:9092     # 一行逗号分隔
     * bootstrap-servers:                                  # 列表，便于逐个加注释
     *   - 10.0.0.1:9092
     *   - 10.0.0.2:9092
     * </pre>
     * 端口可省略（用 {@link #defaultPort}）；非法项打 WARN 跳过，全部非法则启动失败。
     */
    private List<String> bootstrapServers = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getDefaultPort() {
        return defaultPort;
    }

    public void setDefaultPort(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
