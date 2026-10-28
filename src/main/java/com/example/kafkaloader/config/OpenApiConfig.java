package com.example.kafkaloader.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI kafkaLoaderOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Kafka 数据接入服务 API")
                .version("1.0.0")
                .description("Kafka 消息消费 -> 按数据表字段清单生成 CSV 分片文件 -> 本地备份 -> 上传 FTP。"
                        + "集群地址、主题、FTP 等全部配置均来自 application.yml，不读取任何外部配置文件；"
                        + "本服务不连任何数据库，数据最终以文件形式落在 FTP 上，入库由下游负责。")
                .license(new License().name("Internal Use Only")));
    }
}
