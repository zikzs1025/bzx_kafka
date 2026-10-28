package com.example.kafkaloader;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.FtpProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.config.KafkaConsumerProperties;
import com.example.kafkaloader.config.KafkaSecurityProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 应用入口。连接信息全部来自系统配置文件，不依赖任何数据库。
 */
@SpringBootApplication
@EnableConfigurationProperties({
        KafkaClusterProperties.class,
        KafkaConsumerProperties.class,
        KafkaSecurityProperties.class,
        FilePipelineProperties.class,
        FtpProperties.class
})
@EnableScheduling
public class KafkaLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLoaderApplication.class, args);
    }
}
