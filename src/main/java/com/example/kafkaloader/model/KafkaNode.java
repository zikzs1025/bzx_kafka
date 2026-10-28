package com.example.kafkaloader.model;

/**
 * 一个 broker 节点，来自 application.yml 的 {@code kafka.cluster.bootstrap-servers}。
 */
public class KafkaNode {

    private final String ip;
    private final String hostname;
    private final int port;

    public KafkaNode(String ip, String hostname, int port) {
        this.ip = ip;
        this.hostname = hostname;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String bootstrapServer() {
        return ip + ":" + port;
    }

    @Override
    public String toString() {
        return hostname + "@" + bootstrapServer();
    }
}
