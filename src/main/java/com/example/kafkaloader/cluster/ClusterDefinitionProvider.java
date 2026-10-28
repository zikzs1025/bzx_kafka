package com.example.kafkaloader.cluster;

import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.model.ClusterDefinition;
import com.example.kafkaloader.model.KafkaNode;
import com.example.kafkaloader.repository.KafkaNodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 集群定义持有者：启动时把 application.yml 里的节点清单解析成不可变快照，
 * 供消费工厂与运维接口读取。
 * <p>
 * 启动即校验（主题名合法、节点非空），配置有问题直接失败，不会带着空地址去连 Kafka。
 * 运行期配置不再变化，改节点请改 yml 后重启。
 */
public class ClusterDefinitionProvider {

    private static final Logger log = LoggerFactory.getLogger(ClusterDefinitionProvider.class);
    private static final Pattern TOPIC_PATTERN = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    private final KafkaNodeRepository nodeRepository;
    private final KafkaClusterProperties clusterProperties;

    private volatile ClusterDefinition current;

    public ClusterDefinitionProvider(KafkaNodeRepository nodeRepository,
                                     KafkaClusterProperties clusterProperties) {
        this.nodeRepository = nodeRepository;
        this.clusterProperties = clusterProperties;
    }

    @PostConstruct
    public void initialize() {
        ClusterDefinition loaded = load();
        this.current = loaded;
        log.info("已装载 Kafka 集群配置 | cluster={} | topic={} | 节点来源=application.yml | 节点数={} | brokers={}",
                clusterProperties.getName(), loaded.getTopic(),
                loaded.getNodes().size(), loaded.getBootstrapServers());
    }

    public ClusterDefinition get() {
        ClusterDefinition snapshot = this.current;
        if (snapshot == null) {
            throw new IllegalStateException("集群配置尚未初始化");
        }
        return snapshot;
    }

    public String getBootstrapServers() {
        return get().getBootstrapServers();
    }

    private ClusterDefinition load() {
        String topic = clusterProperties.getTopic();
        if (!StringUtils.hasText(topic)) {
            throw new IllegalStateException("kafka.cluster.topic 未配置");
        }
        topic = topic.trim();
        if (!TOPIC_PATTERN.matcher(topic).matches()) {
            throw new IllegalStateException("主题名非法: " + topic);
        }

        List<KafkaNode> nodes = nodeRepository.loadNodes();
        if (nodes.isEmpty()) {
            throw new IllegalStateException("kafka.cluster.bootstrap-servers 未解析出任何 broker 节点");
        }
        return new ClusterDefinition(nodes, topic);
    }
}
