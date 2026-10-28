package com.example.kafkaloader.config;

import com.example.kafkaloader.config.KafkaSecurityProperties.Sasl;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaSecurityAssemblerTest {

    @TempDir
    Path tempDir;

    // ---------------------------------------------------------------- PLAINTEXT

    @Test
    void plaintextShouldOnlyCarryProtocolAndStayCompatible() {
        KafkaSecurityAssembler assembler = new KafkaSecurityAssembler(new KafkaSecurityProperties());

        Map<String, Object> configs = assembler.getClientConfigs();
        assertEquals(1, configs.size());
        assertEquals("PLAINTEXT", configs.get("security.protocol"));
        assertTrue(assembler.isPlaintext());
    }

    // ---------------------------------------------------------------- Kerberos

    @Test
    void kerberosShouldAutoAssembleJaasFromPrincipalAndKeytab() throws Exception {
        Path keytab = Files.createFile(tempDir.resolve("user.keytab"));
        Path krb5 = Files.createFile(tempDir.resolve("krb5.conf"));
        String originalKrb5Prop = System.getProperty("java.security.krb5.conf");
        try {
            KafkaSecurityProperties props = base("SASL_PLAINTEXT");
            Sasl sasl = props.getSasl();
            sasl.setMechanism("GSSAPI");
            sasl.setUsername("user/hive@EXAMPLE.COM");
            sasl.setKeytabLocation(keytab.toString());
            sasl.setKrb5Conf(krb5.toString());
            sasl.setKerberosServiceName("cmkafka");

            Map<String, Object> configs = new KafkaSecurityAssembler(props).getClientConfigs();

            assertEquals("SASL_PLAINTEXT", configs.get("security.protocol"));
            assertEquals("GSSAPI", configs.get(SaslConfigs.SASL_MECHANISM));
            assertEquals("cmkafka", configs.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
            // 客户端 krb5.conf 只能走 JVM 系统属性（kafka-clients 无对应参数），装配期必须已设置
            assertEquals(krb5.toAbsolutePath().normalize().toString(),
                    System.getProperty("java.security.krb5.conf"));

            String jaas = (String) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
            assertTrue(jaas.startsWith("com.sun.security.auth.module.Krb5LoginModule required"));
            assertTrue(jaas.contains("useKeyTab=true"));
            assertTrue(jaas.contains("principal=\"user/hive@EXAMPLE.COM\""));
            // keytab 路径必须是绝对路径（统一正斜杠，避免 JAAS 反斜杠转义）
            assertTrue(jaas.contains("keyTab=\"" + keytab.toAbsolutePath().normalize().toString().replace('\\', '/') + "\""));
        } finally {
            // 还原全局属性，避免污染其他用例
            if (originalKrb5Prop == null) {
                System.clearProperty("java.security.krb5.conf");
            } else {
                System.setProperty("java.security.krb5.conf", originalKrb5Prop);
            }
        }
    }

    @Test
    void kerberosShouldAlwaysCarryDefaultServiceName() throws Exception {
        // 回归用例：即便服务名是默认值 kafka 也必须下发，
        // 否则 SaslChannelBuilder 抛 "No serviceName defined in either JAAS or Kafka config"
        Path keytab = Files.createFile(tempDir.resolve("user2.keytab"));

        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("GSSAPI");
        props.getSasl().setUsername("330100@SYNWAY.COM");
        props.getSasl().setKeytabLocation(keytab.toString());
        props.getSasl().setKerberosServiceName("kafka");

        Map<String, Object> configs = new KafkaSecurityAssembler(props).getClientConfigs();
        assertEquals("kafka", configs.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
    }

    @Test
    void kerberosWithoutKeytabShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("GSSAPI");
        props.getSasl().setUsername("user/hive@EXAMPLE.COM");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("keytab-location"));
    }

    @Test
    void kerberosWithoutPrincipalShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("GSSAPI");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("principal"));
    }

    @Test
    void kerberosMissingKeytabFileShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("GSSAPI");
        props.getSasl().setUsername("user/hive@EXAMPLE.COM");
        props.getSasl().setKeytabLocation(tempDir.resolve("nope.keytab").toString());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("不存在或不可读"));
    }

    // ---------------------------------------------------------------- SCRAM

    @Test
    void scramShouldBuildJaasAndEscapeSpecialChars() {
        KafkaSecurityProperties props = base("SASL_SSL");
        props.getSsl().setTruststoreLocation(createFile("trust.jks"));
        props.getSsl().setTruststorePassword("ts-pwd");
        Sasl sasl = props.getSasl();
        sasl.setMechanism("SCRAM-SHA-512");
        sasl.setUsername("loader");
        sasl.setPassword("p\\w\"d");

        Map<String, Object> configs = new KafkaSecurityAssembler(props).getClientConfigs();
        assertEquals("SCRAM-SHA-512", configs.get(SaslConfigs.SASL_MECHANISM));
        String jaas = (String) configs.get(SaslConfigs.SASL_JAAS_CONFIG);
        assertTrue(jaas.startsWith("org.apache.kafka.common.security.scram.ScramLoginModule required"));
        assertTrue(jaas.contains("username=\"loader\""));
        // 引号与反斜杠必须转义，否则 JAAS 解析失败
        assertTrue(jaas.contains("password=\"p\\\\w\\\"d\""));
        assertEquals("trust.jks".replace("/", java.io.File.separatorChar == '/' ? "/" : "\\"),
                name((String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)));
    }

    @Test
    void scramWithoutPasswordShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("SCRAM-SHA-256");
        props.getSasl().setUsername("loader");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("username / password"));
    }

    // ---------------------------------------------------------------- 校验分支

    @Test
    void saslProtocolWithoutMechanismShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("sasl.mechanism"));
    }

    @Test
    void invalidProtocolShouldFailFast() {
        KafkaSecurityProperties props = base("TLS_ONLY");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("protocol 非法"));
    }

    @Test
    void unsupportedMechanismShouldFailFast() {
        KafkaSecurityProperties props = base("SASL_PLAINTEXT");
        props.getSasl().setMechanism("OAUTHBEARER");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("不支持的"));
    }

    @Test
    void pemKeystoreShouldBeRejectedWithConvertHint() {
        KafkaSecurityProperties props = base("SSL");
        props.getSsl().setKeystoreLocation(createFile("client.p12"));
        props.getSsl().setKeystoreType("PEM");
        props.getSsl().setKeystorePassword("x");

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("PKCS12"));
    }

    @Test
    void keystoreWithoutPasswordShouldFailFast() {
        KafkaSecurityProperties props = base("SSL");
        props.getSsl().setKeystoreLocation(createFile("client.p12"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("keystore-password"));
    }

    @Test
    void missingTruststoreFileShouldFailFast() {
        KafkaSecurityProperties props = base("SSL");
        props.getSsl().setTruststoreLocation(tempDir.resolve("nope.jks").toString());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new KafkaSecurityAssembler(props));
        assertTrue(ex.getMessage().contains("不存在或不可读"));
    }

    @Test
    void summaryShouldNeverContainPasswords() {
        KafkaSecurityProperties props = base("SASL_SSL");
        props.getSsl().setTruststoreLocation(createFile("trust.jks"));
        props.getSsl().setTruststorePassword("SECRET-TS");
        Sasl sasl = props.getSasl();
        sasl.setMechanism("SCRAM-SHA-512");
        sasl.setUsername("loader");
        sasl.setPassword("SECRET-PWD");

        String summary = new KafkaSecurityAssembler(props).getSummary();
        assertTrue(summary.contains("protocol=SASL_SSL"));
        assertTrue(summary.contains("user=loader"));
        assertTrue(!summary.contains("SECRET-TS"));
        assertTrue(!summary.contains("SECRET-PWD"));
    }

    // ---------------------------------------------------------------- helpers

    private static KafkaSecurityProperties base(String protocol) {
        KafkaSecurityProperties props = new KafkaSecurityProperties();
        props.setProtocol(protocol);
        return props;
    }

    private String createFile(String name) {
        try {
            return Files.createFile(tempDir.resolve(name)).toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private String name(String path) {
        return path.substring(path.replace('\\', '/').lastIndexOf('/') + 1);
    }
}
