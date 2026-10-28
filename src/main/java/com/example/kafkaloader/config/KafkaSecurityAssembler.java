package com.example.kafkaloader.config;

import com.example.kafkaloader.config.KafkaSecurityProperties.Sasl;
import com.example.kafkaloader.config.KafkaSecurityProperties.Ssl;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 把 {@link KafkaSecurityProperties} 组装成 Kafka 客户端安全参数。
 * <p>
 * 构造期即完成校验：协议与机制不匹配、证书/keytab 文件不存在、密码缺失等问题
 * 直接启动失败并给出中文原因，绝不带着半套安全配置去连 broker 再抛难懂的握手异常。
 * <p>
 * 密码只存在于返回的 configs 里；{@link #getSummary()} 与日志输出全部脱敏。
 */
@Component
public class KafkaSecurityAssembler {

    private static final Logger log = LoggerFactory.getLogger(KafkaSecurityAssembler.class);

    private static final Set<String> PROTOCOLS =
            new HashSet<>(Arrays.asList("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"));
    private static final Set<String> SASL_MECHANISMS =
            new HashSet<>(Arrays.asList("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"));
    private static final String PLAIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule";
    private static final String SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";
    private static final String KRB5_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    private final KafkaSecurityProperties properties;
    private final String protocol;
    private final Map<String, Object> clientConfigs;
    private final String summary;

    public KafkaSecurityAssembler(KafkaSecurityProperties properties) {
        this.properties = properties;
        this.protocol = normalizeProtocol(properties.getProtocol());
        this.clientConfigs = Collections.unmodifiableMap(build());
        this.summary = describe();
        log.info("Kafka 连接安全配置 | {}", summary);
    }

    /** 安全参数（含密码），只供 ConsumerFactory 注入，禁止日志输出 */
    public Map<String, Object> getClientConfigs() {
        return clientConfigs;
    }

    /** 脱敏摘要，可直接回显到运维接口与日志 */
    public String getSummary() {
        return summary;
    }

    public boolean isPlaintext() {
        return "PLAINTEXT".equals(protocol);
    }

    private Map<String, Object> build() {
        Map<String, Object> configs = new LinkedHashMap<>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        if (protocol.contains("SSL")) {
            fillSsl(configs);
        }
        if (protocol.startsWith("SASL")) {
            fillSasl(configs);
        }
        return configs;
    }

    private void fillSsl(Map<String, Object> configs) {
        Ssl ssl = properties.getSsl();

        String truststore = resolve(ssl.getTruststoreLocation(), "ssl.truststore-location");
        if (truststore == null) {
            log.warn("未配置 ssl.truststore-location，将使用 JDK 默认信任库 cacerts；"
                    + "broker 为自签或内部 CA 证书时握手一定失败");
        } else {
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
            configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, nvl(ssl.getTruststoreType(), "JKS"));
            if (StringUtils.hasText(ssl.getTruststorePassword())) {
                configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.getTruststorePassword());
            }
        }

        String keystore = resolve(ssl.getKeystoreLocation(), "ssl.keystore-location");
        if (keystore != null) {
            String keystoreType = nvl(ssl.getKeystoreType(), "JKS");
            if ("PEM".equalsIgnoreCase(keystoreType)) {
                throw new IllegalStateException("ssl.keystore.type=PEM 需内联 ssl.keystore.certificate.chain / "
                        + "ssl.keystore.key，不支持按路径读取；请转成 PKCS12：openssl pkcs12 -export "
                        + "-in client.crt -inkey client.key -out client.p12");
            }
            if (!StringUtils.hasText(ssl.getKeystorePassword())) {
                throw new IllegalStateException("已配置 ssl.keystore-location（双向认证），但 ssl.keystore-password 为空");
            }
            configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
            configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystoreType);
            configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.getKeystorePassword());
            configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                    StringUtils.hasText(ssl.getKeyPassword()) ? ssl.getKeyPassword() : ssl.getKeystorePassword());
        }

        // 空串表示关闭主机名校验，这是 Kafka 客户端认的语义
        configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                nvl(ssl.getEndpointIdentificationAlgorithm(), "https"));
        if (StringUtils.hasText(ssl.getProtocol())) {
            configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, ssl.getProtocol());
        }
        if (StringUtils.hasText(ssl.getEnabledProtocols())) {
            configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, ssl.getEnabledProtocols());
        }
    }

    private void fillSasl(Map<String, Object> configs) {
        Sasl sasl = properties.getSasl();

        String mechanism = sasl.getMechanism() == null ? "" : sasl.getMechanism().trim().toUpperCase(Locale.ROOT);
        if (mechanism.isEmpty()) {
            throw new IllegalStateException("security.protocol=" + protocol
                    + " 需要配置 kafka.cluster.security.sasl.mechanism（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / GSSAPI）");
        }
        if (!SASL_MECHANISMS.contains(mechanism)) {
            throw new IllegalStateException("不支持的 kafka.cluster.security.sasl.mechanism: " + sasl.getMechanism()
                    + "（可选 PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / GSSAPI）");
        }
        configs.put(SaslConfigs.SASL_MECHANISM, mechanism);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaas(mechanism, sasl));

        if ("GSSAPI".equals(mechanism)) {
            if (StringUtils.hasText(sasl.getKrb5Conf())) {
                applyKrb5Conf(sasl.getKrb5Conf());
            }
            // 必须显式下发：SaslChannelBuilder 会校验该键（缺失即抛
            // IllegalArgumentException: No serviceName defined in either JAAS or Kafka config）。
            // 日志中 "isn't a known config" 的 WARN 属正常噪音（该键不在 ConsumerConfig 已知列表，
            // 但 SaslChannelBuilder 仍会从原始配置中读取），不可据此省略。
            if (StringUtils.hasText(sasl.getKerberosServiceName())) {
                configs.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, sasl.getKerberosServiceName().trim());
            }
        }
    }

    /**
     * kafka-clients 的消费端没有 sasl.kerberos.krb5.conf 参数（那是 broker 端的），
     * 客户端只能读 JVM 系统属性 java.security.krb5.conf。
     * 这里在装配期（早于任何 Krb5 登录）直接设置，部署时不必再手动传 -D。
     */
    private void applyKrb5Conf(String raw) {
        String path = resolve(raw, "sasl.krb5-conf");
        String previous = System.setProperty("java.security.krb5.conf", path);
        if (!path.equals(previous)) {
            log.info("已设置 java.security.krb5.conf={}", path);
        }
        // 若 JVM 内已有缓存的 Kerberos 配置（理论上装配期不会发生），尽力刷新
        try {
            Class<?> config = Class.forName("sun.security.krb5.Config");
            config.getMethod("refresh").invoke(config.getMethod("getInstance").invoke(null));
        } catch (Throwable ignored) {
            // 非 HotSpot / JDK 裁剪版：忽略，系统属性仍会在首次登录时生效
        }
    }

    private String buildJaas(String mechanism, Sasl sasl) {
        if (StringUtils.hasText(sasl.getJaasConfig())) {
            return sasl.getJaasConfig().trim();
        }
        if ("GSSAPI".equals(mechanism)) {
            return buildKrb5Jaas(sasl);
        }
        if (!StringUtils.hasText(sasl.getUsername()) || !StringUtils.hasText(sasl.getPassword())) {
            throw new IllegalStateException("SASL " + mechanism + " 认证需要 kafka.cluster.security.sasl.username / "
                    + "password（建议走环境变量），或直接提供 sasl.jaas-config");
        }
        String module = "PLAIN".equals(mechanism) ? PLAIN_MODULE : SCRAM_MODULE;
        return module + " required username=\"" + escape(sasl.getUsername())
                + "\" password=\"" + escape(sasl.getPassword()) + "\";";
    }

    /**
     * Kerberos：由 principal + keytab 自动拼 Krb5LoginModule 的 JAAS 串，
     * 省去手写 jaas-config 的路径转义问题。
     */
    private String buildKrb5Jaas(Sasl sasl) {
        if (!StringUtils.hasText(sasl.getUsername())) {
            throw new IllegalStateException("Kerberos(GSSAPI) 认证需要 kafka.cluster.security.sasl.username"
                    + " 填 principal（如 user/hive@EXAMPLE.COM），或直接提供 sasl.jaas-config");
        }
        if (!sasl.getUsername().contains("@")) {
            // 缺 realm 时靠 krb5.conf 的 default_realm 兜底能登录，但 broker 侧身份与 ACL 登记易对不上
            log.warn("Kerberos principal 未带 realm: {}，建议写成 用户名@REALM（如 330100@SYNWAY.COM），"
                    + "否则 ACL 授权时身份字符串可能与预期不一致", sasl.getUsername());
        }
        if (!StringUtils.hasText(sasl.getKeytabLocation())) {
            throw new IllegalStateException("Kerberos(GSSAPI) 认证需要 kafka.cluster.security.sasl.keytab-location"
                    + " 指向 keytab 文件，或直接提供 sasl.jaas-config");
        }
        // 统一正斜杠：Windows 反斜杠在 JAAS 里需转义，正斜杠两端平台都接受
        String keytab = requireFile(sasl.getKeytabLocation(), "sasl.keytab-location").replace('\\', '/');
        return KRB5_MODULE + " required useKeyTab=true storeKey=true useTicketCache=false"
                + " keyTab=\"" + escape(keytab) + "\""
                + " principal=\"" + escape(sasl.getUsername()) + "\";";
    }

    private String describe() {
        StringBuilder sb = new StringBuilder("protocol=").append(protocol);
        if (protocol.startsWith("SASL")) {
            Sasl sasl = properties.getSasl();
            sb.append(" | mechanism=").append(nvl(sasl.getMechanism(), "-"));
            if ("GSSAPI".equals(nvl(sasl.getMechanism(), "").trim().toUpperCase(Locale.ROOT))) {
                sb.append(" | principal=").append(nvl(sasl.getUsername(), "-"));
                sb.append(" | keytab=").append(fileName(sasl.getKeytabLocation(), "-"));
                sb.append(" | krb5=").append(fileName(sasl.getKrb5Conf(), "JVM 默认"));
                sb.append(" | serviceName=").append(nvl(sasl.getKerberosServiceName(), "kafka"));
            } else if (StringUtils.hasText(sasl.getUsername())) {
                sb.append(" | user=").append(sasl.getUsername());
            }
            sb.append(StringUtils.hasText(sasl.getJaasConfig()) ? " | jaas=inline" : " | jaas=auto");
        }
        if (protocol.contains("SSL")) {
            Ssl ssl = properties.getSsl();
            sb.append(" | truststore=").append(fileName(ssl.getTruststoreLocation(), "JDK 默认"));
            sb.append(" | keystore=").append(fileName(ssl.getKeystoreLocation(), "无（单向认证）"));
        }
        return sb.toString();
    }

    /** 相对路径按进程工作目录解析（start.sh 会先 cd 到 dist 根），存在性延后到具体场景校验 */
    private static String resolve(String raw, String key) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        return requireFile(raw, key);
    }

    private static String requireFile(String raw, String key) {
        Path path = Paths.get(raw.trim()).toAbsolutePath().normalize();
        if (!Files.isReadable(path)) {
            throw new IllegalStateException("kafka.cluster.security." + key + " 指向的文件不存在或不可读: " + path);
        }
        return path.toString();
    }

    private static String normalizeProtocol(String raw) {
        String value = StringUtils.hasText(raw) ? raw.trim().toUpperCase(Locale.ROOT) : "PLAINTEXT";
        if (!PROTOCOLS.contains(value)) {
            throw new IllegalStateException("kafka.cluster.security.protocol 非法: " + raw
                    + "（可选 PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL）");
        }
        return value;
    }

    /** JAAS 串里引号、反斜杠必须转义，否则密码/principal 含特殊字符时登录失败 */
    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String fileName(String raw, String fallback) {
        if (!StringUtils.hasText(raw)) {
            return fallback;
        }
        return Paths.get(raw.trim()).getFileName().toString();
    }

    private static String nvl(String value, String fallback) {
        return value == null ? fallback : value;
    }
}
