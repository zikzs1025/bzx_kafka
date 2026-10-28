package com.example.kafkaloader.repository;

import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.model.KafkaNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigKafkaNodeRepositoryTest {

    private ConfigKafkaNodeRepository repository(String... entries) {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        properties.setDefaultPort(9092);
        properties.setBootstrapServers(new ArrayList<>(Arrays.asList(entries)));
        return new ConfigKafkaNodeRepository(properties);
    }

    @Test
    void shouldParseYamlListStyle() {
        List<KafkaNode> nodes = repository("10.100.1.180:9092", "10.100.1.181:9093").loadNodes();

        assertEquals(2, nodes.size());
        assertEquals("10.100.1.180:9092", nodes.get(0).bootstrapServer());
        assertEquals("10.100.1.181:9093", nodes.get(1).bootstrapServer());
    }

    @Test
    void shouldParseCommaSeparatedStyle() {
        // Spring 把 'a,b' 绑成单元素 List 时，这里要能再拆一次
        List<KafkaNode> nodes = repository("10.100.1.180:9092,10.100.1.181:9092").loadNodes();

        assertEquals(2, nodes.size());
        assertEquals("10.100.1.180:9092", nodes.get(0).bootstrapServer());
        assertEquals("10.100.1.181:9092", nodes.get(1).bootstrapServer());
    }

    @Test
    void shouldUseDefaultPortWhenOmitted() {
        List<KafkaNode> nodes = repository("10.100.1.182").loadNodes();

        assertEquals(1, nodes.size());
        assertEquals("10.100.1.182:9092", nodes.get(0).bootstrapServer());
        assertEquals(9092, nodes.get(0).getPort());
    }

    @Test
    void shouldTreatHostNameAsHostWhenNoIpGiven() {
        KafkaNode node = repository("kafka01:9092").loadNodes().get(0);

        assertEquals("kafka01", node.getIp());
        assertEquals("kafka01", node.getHostname());
        assertEquals(9092, node.getPort());
    }

    @Test
    void shouldDeduplicateSameHostAndPort() {
        List<KafkaNode> nodes = repository(
                "10.100.1.180:9092", "10.100.1.180:9092", " 10.100.1.180:9092 ", "10.100.1.181:9092").loadNodes();

        assertEquals(2, nodes.size());
        assertEquals("10.100.1.180:9092", nodes.get(0).bootstrapServer());
    }

    @Test
    void shouldKeepIpv6BracketLiteral() {
        List<KafkaNode> nodes = repository("[::1]:9093").loadNodes();

        assertEquals(1, nodes.size());
        assertEquals("[::1]:9093", nodes.get(0).bootstrapServer());
        assertEquals(9093, nodes.get(0).getPort());
    }

    @Test
    void shouldSkipIllegalEntryButKeepValidOnes() {
        List<KafkaNode> nodes = repository(
                "10.100.1.180:9092", "10.100.1.181:70000", "bad host!", ":9092", "10.100.1.182:9092").loadNodes();

        assertEquals(2, nodes.size());
        assertEquals("10.100.1.180:9092", nodes.get(0).bootstrapServer());
        assertEquals("10.100.1.182:9092", nodes.get(1).bootstrapServer());
    }

    @Test
    void shouldSkipPortNotNumeric() {
        assertEquals(1, repository("10.100.1.180:abc", "10.100.1.181:9092").loadNodes().size());
    }

    @Test
    void shouldFailWhenNothingConfigured() {
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> repository().loadNodes());
        assertTrue(error.getMessage().contains("未配置 Kafka 节点"), error.getMessage());
    }

    @Test
    void shouldFailWhenAllEntriesIllegal() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> repository("bad host!", "10.100.1.181:70000").loadNodes());
        assertTrue(error.getMessage().contains("没有任何有效节点"), error.getMessage());
    }

    @Test
    void shouldFallbackTo9092WhenDefaultPortOutOfRange() {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        properties.setDefaultPort(0);
        properties.setBootstrapServers(new ArrayList<>(Arrays.asList("10.100.1.182")));

        assertEquals("10.100.1.182:9092", new ConfigKafkaNodeRepository(properties).loadNodes()
                .get(0).bootstrapServer());
    }
}
