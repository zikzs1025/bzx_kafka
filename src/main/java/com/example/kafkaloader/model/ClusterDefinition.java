package com.example.kafkaloader.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 集群定义快照：节点列表 + 主题。由 application.yml 解析生成，构造后不可变。
 */
public class ClusterDefinition {

    private final List<KafkaNode> nodes;
    private final String topic;

    public ClusterDefinition(List<KafkaNode> nodes, String topic) {
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.topic = topic;
    }

    public List<KafkaNode> getNodes() {
        return nodes;
    }

    public String getTopic() {
        return topic;
    }

    public String getBootstrapServers() {
        return nodes.stream()
                .map(KafkaNode::bootstrapServer)
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "ClusterDefinition{nodes=" + nodes + ", topic='" + topic + "'}";
    }
}
