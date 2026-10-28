package com.example.kafkaloader.repository;

import com.example.kafkaloader.model.KafkaNode;

import java.util.List;

/**
 * 节点来源抽象：把「broker 从哪来」的细节关在这里，上层只认节点列表。
 * <p>
 * 当前实现 {@link ConfigKafkaNodeRepository} 从 application.yml 读取；
 * 若将来换成配置中心，只需再实现一次本接口。
 */
public interface KafkaNodeRepository {

    /**
     * 读取配置中的 broker 节点，按 yml 里的书写顺序返回。
     *
     * @throws IllegalStateException 未配置或内容全部非法
     */
    List<KafkaNode> loadNodes();
}
