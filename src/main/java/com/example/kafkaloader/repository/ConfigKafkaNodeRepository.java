package com.example.kafkaloader.repository;

import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.model.KafkaNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 从 application.yml 的 {@code kafka.cluster.bootstrap-servers} 读取 broker 节点。
 * <p>
 * 支持两种书写形式（Spring 的 List 绑定都能吃下）：
 * <pre>
 * bootstrap-servers: 10.100.1.180:9092,10.100.1.181:9092
 * bootstrap-servers:
 *   - 10.100.1.180:9092
 *   - 10.100.1.181
 * </pre>
 * 解析规则：
 * <ul>
 *   <li>端口省略时用 {@code kafka.cluster.default-port}</li>
 *   <li>IPv6 字面量保留方括号（{@code [::1]:9092}），这是 Kafka 唯一认的写法</li>
 *   <li>同一 {@code host:port} 重复出现只保留一次</li>
 *   <li>单条非法（端口非数字、超范围、主机名非法）打 WARN 跳过，不影响其他节点</li>
 *   <li>一条都解析不出来时抛异常让启动失败——绝不带着空地址去连 Kafka</li>
 * </ul>
 * 配置改动需重启进程生效。
 */
public class ConfigKafkaNodeRepository implements KafkaNodeRepository {

    private static final Logger log = LoggerFactory.getLogger(ConfigKafkaNodeRepository.class);

    /** 普通主机名：字母、数字、点、下划线、连字符；不允许冒号（冒号只能作为 host:port 分隔符） */
    private static final Pattern HOST_PATTERN = Pattern.compile("^[A-Za-z0-9._-]{1,255}$");

    /** IPv6 字面量（含方括号）：仅允许十六进制、冒号、点与可选的 %zone */
    private static final Pattern IPV6_PATTERN = Pattern.compile("^\\[([0-9A-Fa-f:.]+)(%[A-Za-z0-9._-]+)?\\]$");

    private static final int FALLBACK_PORT = 9092;

    private final KafkaClusterProperties properties;

    public ConfigKafkaNodeRepository(KafkaClusterProperties properties) {
        this.properties = properties;
    }

    @Override
    public List<KafkaNode> loadNodes() {
        List<String> configured = properties.getBootstrapServers();
        if (configured == null || configured.isEmpty()) {
            throw new IllegalStateException(
                    "未配置 Kafka 节点：请在 application.yml 里填写 kafka.cluster.bootstrap-servers");
        }

        List<KafkaNode> nodes = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String entry : configured) {
            if (entry == null) {
                continue;
            }
            // 一行里写多个地址（逗号分隔）也允许，这里再拆一次
            for (String token : entry.split(",")) {
                String value = token.trim();
                if (value.isEmpty()) {
                    continue;
                }
                KafkaNode node = toNode(value, "kafka.cluster.bootstrap-servers");
                if (node != null && seen.add(node.bootstrapServer())) {
                    nodes.add(node);
                }
            }
        }

        if (nodes.isEmpty()) {
            throw new IllegalStateException("kafka.cluster.bootstrap-servers 里没有任何有效节点，"
                    + "请检查格式（应为 IP[:端口]，多个用逗号分隔或写成列表）");
        }
        log.debug("已从配置解析 {} 个 broker 节点: {}", nodes.size(), new ArrayList<>(seen));
        return nodes;
    }

    /**
     * 把 {@code host[:port]} 转成节点，非法输入返回 null（已打 WARN）。
     */
    private KafkaNode toNode(String value, String where) {
        String host = value;
        Integer port = null;

        if (value.startsWith("[")) {
            // IPv6 字面量：[::1]:9092，方括号要保留
            int close = value.indexOf(']');
            if (close < 0) {
                log.warn("节点地址 [{}] IPv6 括号未闭合，已跳过（{}）", value, where);
                return null;
            }
            host = value.substring(0, close + 1);
            String rest = value.substring(close + 1);
            if (rest.startsWith(":")) {
                String portText = rest.substring(1).trim();
                if (!portText.isEmpty()) {
                    port = parsePort(portText, value, where);
                    if (port == null) {
                        return null;
                    }
                }
            }
        } else {
            int colon = value.lastIndexOf(':');
            if (colon > 0 && value.indexOf(':') == colon) {
                host = value.substring(0, colon);
                String portText = value.substring(colon + 1).trim();
                // 只写了 host: 时按缺省端口处理，不算错误
                if (!portText.isEmpty()) {
                    port = parsePort(portText, value, where);
                    if (port == null) {
                        return null;
                    }
                }
            }
        }

        host = host.trim();
        boolean ipv6 = host.startsWith("[");
        boolean hostOk = ipv6 ? IPV6_PATTERN.matcher(host).matches() : HOST_PATTERN.matcher(host).matches();
        if (host.isEmpty() || !hostOk) {
            log.warn("节点地址 [{}] 主机名非法，已跳过（{}）", value, where);
            return null;
        }
        return new KafkaNode(host, host, port == null ? defaultPort() : port);
    }

    private Integer parsePort(String raw, String original, String where) {
        try {
            int port = Integer.parseInt(raw.trim());
            if (port <= 0 || port > 65535) {
                log.warn("节点地址 [{}] 端口超出 1~65535，已跳过（{}）", original, where);
                return null;
            }
            return port;
        } catch (NumberFormatException e) {
            log.warn("节点地址 [{}] 端口不是数字，已跳过（{}）", original, where);
            return null;
        }
    }

    private int defaultPort() {
        int port = properties.getDefaultPort();
        return port > 0 && port <= 65535 ? port : FALLBACK_PORT;
    }
}
