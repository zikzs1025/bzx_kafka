package com.example.kafkaloader.config;

import com.example.kafkaloader.cluster.ClusterDefinitionProvider;
import com.example.kafkaloader.repository.ConfigKafkaNodeRepository;
import com.example.kafkaloader.repository.KafkaNodeRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 集群配置装配：节点全部来自 application.yml，应用不需要任何数据库连接。
 */
@Configuration
public class ClusterConfig {

    @Bean
    public KafkaNodeRepository kafkaNodeRepository(KafkaClusterProperties clusterProperties) {
        return new ConfigKafkaNodeRepository(clusterProperties);
    }

    @Bean
    public ClusterDefinitionProvider clusterDefinitionProvider(KafkaNodeRepository nodeRepository,
                                                               KafkaClusterProperties clusterProperties) {
        return new ClusterDefinitionProvider(nodeRepository, clusterProperties);
    }
}
