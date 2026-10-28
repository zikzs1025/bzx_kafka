package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka 连接安全配置（证书 / SASL 认证）。
 * <p>
 * 所有字段留空（protocol 保持默认 PLAINTEXT）时与历史版本行为完全一致，
 * 对既有明文环境零影响。现场 Kerberos 场景只需：
 * <pre>
 *   kafka.cluster.security.protocol=SASL_PLAINTEXT
 *   kafka.cluster.security.sasl.mechanism=GSSAPI
 *   kafka.cluster.security.sasl.username=principal 名
 *   kafka.cluster.security.sasl.keytab-location=keytab 路径
 *   kafka.cluster.security.sasl.krb5-conf=krb5.conf 路径
 * </pre>
 */
@ConfigurationProperties(prefix = "kafka.cluster.security")
public class KafkaSecurityProperties {

    /** 安全协议：PLAINTEXT(默认) | SSL | SASL_PLAINTEXT | SASL_SSL */
    private String protocol = "PLAINTEXT";

    private final Ssl ssl = new Ssl();
    private final Sasl sasl = new Sasl();

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Ssl getSsl() {
        return ssl;
    }

    public Sasl getSasl() {
        return sasl;
    }

    /**
     * TLS 证书配置：单向认证只配 truststore；双向认证（mTLS）再加 keystore。
     * Kerberos(SASL_PLAINTEXT) 场景整段留空即可。
     */
    public static class Ssl {

        /** CA 信任库路径。留空 = 用 JDK 默认 cacerts（仅 broker 为公共 CA 签发时可用） */
        private String truststoreLocation;

        private String truststorePassword = "";

        /** JKS | PKCS12 | PEM */
        private String truststoreType = "JKS";

        /** 客户端证书库，仅双向认证需要；留空 = 单向认证 */
        private String keystoreLocation;

        private String keystorePassword = "";

        /** JKS | PKCS12（PEM 需内联证书内容，不支持按路径读取，配置了直接报错） */
        private String keystoreType = "JKS";

        /** 私钥口令，留空复用 keystorePassword */
        private String keyPassword = "";

        /** 主机名校验算法，https=严格；空串=关闭（broker 用 IP 且证书只签了域名时使用，有中间人风险） */
        private String endpointIdentificationAlgorithm = "https";

        private String protocol = "TLSv1.2";

        private String enabledProtocols = "TLSv1.2,TLSv1.3";

        public String getTruststoreLocation() {
            return truststoreLocation;
        }

        public void setTruststoreLocation(String truststoreLocation) {
            this.truststoreLocation = truststoreLocation;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }

        public String getTruststoreType() {
            return truststoreType;
        }

        public void setTruststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
        }

        public String getKeystoreLocation() {
            return keystoreLocation;
        }

        public void setKeystoreLocation(String keystoreLocation) {
            this.keystoreLocation = keystoreLocation;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
        }

        public String getKeystoreType() {
            return keystoreType;
        }

        public void setKeystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public String getEndpointIdentificationAlgorithm() {
            return endpointIdentificationAlgorithm;
        }

        public void setEndpointIdentificationAlgorithm(String endpointIdentificationAlgorithm) {
            this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
        }

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getEnabledProtocols() {
            return enabledProtocols;
        }

        public void setEnabledProtocols(String enabledProtocols) {
            this.enabledProtocols = enabledProtocols;
        }
    }

    /** SASL 认证配置 */
    public static class Sasl {

        /** PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI(Kerberos) */
        private String mechanism;

        /**
         * PLAIN / SCRAM：登录用户名。
         * GSSAPI：Kerberos principal（如 user/hive@EXAMPLE.COM），与 keytab 配套。
         */
        private String username;

        /** PLAIN / SCRAM 的登录密码；GSSAPI 不使用 */
        private String password;

        /**
         * 完整 JAAS 串，填了则优先于其他字段自动拼装。
         * 例：com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/path/user.keytab" principal="u@R" storeKey=true;
         */
        private String jaasConfig;

        /** GSSAPI：keytab 文件路径，与 username(principal) 配套；留空且无 jaas-config 则报错 */
        private String keytabLocation;

        /** GSSAPI：krb5.conf 路径，对应客户端参数 sasl.kerberos.krb5.conf；留空用 JVM 默认 */
        private String krb5Conf;

        /** GSSAPI：broker 端 Kerberos 服务名（server.properties 的 sasl.kerberos.service.name） */
        private String kerberosServiceName = "kafka";

        public String getMechanism() {
            return mechanism;
        }

        public void setMechanism(String mechanism) {
            this.mechanism = mechanism;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getJaasConfig() {
            return jaasConfig;
        }

        public void setJaasConfig(String jaasConfig) {
            this.jaasConfig = jaasConfig;
        }

        public String getKeytabLocation() {
            return keytabLocation;
        }

        public void setKeytabLocation(String keytabLocation) {
            this.keytabLocation = keytabLocation;
        }

        public String getKrb5Conf() {
            return krb5Conf;
        }

        public void setKrb5Conf(String krb5Conf) {
            this.krb5Conf = krb5Conf;
        }

        public String getKerberosServiceName() {
            return kerberosServiceName;
        }

        public void setKerberosServiceName(String kerberosServiceName) {
            this.kerberosServiceName = kerberosServiceName;
        }
    }
}
