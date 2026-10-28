# Kafka 证书 + 认证改造方案

> 目标：当前只有 `bootstrap.servers` 明文连接，需要支持 broker 开启 SSL（含双向认证）与 SASL 用户名口令认证。
> 原则：**不破坏现有明文环境**（`security.protocol` 默认 `PLAINTEXT`），安全配置项全部走 `application.yml` + 环境变量，证书文件外置不进 jar、不进 git。

## 一、改动清单

| # | 文件 | 动作 | 说明 |
|---|---|---|---|
| 1 | `config/KafkaSecurityProperties.java` | 新增 | `kafka.cluster.security.*` 配置绑定 |
| 2 | `config/KafkaSecurityAssembler.java` | 新增 | 组装 Kafka 客户端安全参数 + 启动期校验（fail fast） |
| 3 | `config/KafkaConsumerConfig.java` | 改 1 处 | `configs.putAll(security.getClientConfigs())` |
| 4 | `KafkaLoaderApplication.java` | 改 1 行 | `@EnableConfigurationProperties` 增加新类 |
| 5 | `resources/application.yml` | 加一段 | `kafka.cluster.security`（默认 PLAINTEXT，零影响） |
| 6 | `api/AdminController.java` | 加 1 行 | `/api/admin/status` 回显安全配置摘要（不含密码） |
| 7 | `src/assembly/dist.xml` | 加 fileSet | 分发 `config/certs/` 占位目录 |
| 8 | `bin/start.sh` | 小改 | 支持 `JAVA_OPTS`（Kerberos 需要传 `-Djava.security.krb5.conf`） |
| 9 | `.gitignore` | 加 4 行 | 证书、私钥、信任库不进版本库 |
| 10 | `src/test/.../KafkaSecurityAssemblerTest.java` | 新增 | 6 个用例覆盖校验分支 |

不动的部分：`RefreshingConsumerFactory`、`ClusterDefinitionProvider`、文件管道、FTP 全链路。`bootstrap.servers` 的解析逻辑不变，**只改端口**。

---

## 二、配置模型

`application.yml` 新增（默认值即现网明文行为，不改也能启动）：

```yaml
kafka:
  cluster:
    # 注意：开启 SSL/SASL 后这里的端口必须换成 broker 的安全监听端口（常见 9093 / 9094），
    # 不是现在的 9092，否则会一直报「Connection to node terminated during authentication」
    bootstrap-servers:
      - 10.100.1.180:9093
      # ...

    security:
      # PLAINTEXT(默认) | SSL | SASL_PLAINTEXT | SASL_SSL
      protocol: SASL_SSL

      ssl:
        # CA 信任库。留空 = 用 JDK 默认 cacerts（仅当 broker 用公共 CA 签发才够）
        truststore-location: ./config/certs/kafka.truststore.jks
        truststore-password: ${KAFKA_TRUSTSTORE_PASSWORD:}
        truststore-type: JKS          # JKS | PKCS12 | PEM（PEM 直接指向 .pem 文件）
        # 客户端证书库：仅双向认证（mTLS）需要，单向认证留空
        keystore-location: ./config/certs/kafka.keystore.jks
        keystore-password: ${KAFKA_KEYSTORE_PASSWORD:}
        keystore-type: JKS
        key-password: ${KAFKA_KEY_PASSWORD:}       # 留空则复用 keystore-password
        # 主机名校验：https=严格校验；置空串=不校验（broker 用 IP 连接、证书只签了域名时用，有中间人风险，仅限内网）
        endpoint-identification-algorithm: https
        protocol: TLSv1.2
        enabled-protocols: TLSv1.2,TLSv1.3

      sasl:
        # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | GSSAPI(Kerberos)
        mechanism: SCRAM-SHA-512
        username: ${KAFKA_SASL_USERNAME:}
        password: ${KAFKA_SASL_PASSWORD:}
        # 完整 JAAS 串，填了就优先于上面的 username/password；Kerberos 必须走这里
        jaas-config: ""
```

三种典型组合：

| 现场情况 | protocol | 必填项 |
|---|---|---|
| 只有服务端证书（单向 TLS） | `SSL` | `ssl.truststore-*` |
| 单向 TLS + 用户名口令 | `SASL_SSL` | `ssl.truststore-*` + `sasl.mechanism/username/password` |
| 双向认证（mTLS，政务/公安常见） | `SSL` 或 `SASL_SSL` | 上述 + `ssl.keystore-*` |
| Kerberos | `SASL_SSL` | `sasl.mechanism=GSSAPI` + `sasl.jaas-config` + `JAVA_OPTS` 传 krb5.conf |

密码**只走环境变量**，yml 里写 `${ENV:}` 占位，不落明文：

```bash
export KAFKA_TRUSTSTORE_PASSWORD='xxx'
export KAFKA_SASL_USERNAME='loader'
export KAFKA_SASL_PASSWORD='xxx'
./bin/start.sh restart
```

---

## 三、代码实现

### 3.1 新增 `KafkaSecurityProperties`

```java
package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka 连接安全配置（证书 / SASL 认证）。
 * 全部留空时等价于原有 PLAINTEXT 明文连接，对既有环境零影响。
 */
@ConfigurationProperties(prefix = "kafka.cluster.security")
public class KafkaSecurityProperties {

    /** PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL */
    private String protocol = "PLAINTEXT";

    private final Ssl ssl = new Ssl();
    private final Sasl sasl = new Sasl();

    public String getProtocol() { return protocol; }
    public void setProtocol(String protocol) { this.protocol = protocol; }
    public Ssl getSsl() { return ssl; }
    public Sasl getSasl() { return sasl; }

    /** 证书配置：单向认证只配 truststore，双向认证再加 keystore */
    public static class Ssl {
        private String truststoreLocation;
        private String truststorePassword = "";
        private String truststoreType = "JKS";
        private String keystoreLocation;
        private String keystorePassword = "";
        private String keystoreType = "JKS";
        private String keyPassword = "";
        private String endpointIdentificationAlgorithm = "https";
        private String protocol = "TLSv1.2";
        private String enabledProtocols = "TLSv1.2,TLSv1.3";

        // getters / setters 省略，字段名与 yml 的 kebab-case 一一对应
    }

    /** 认证配置 */
    public static class Sasl {
        private String mechanism;
        private String username;
        private String password;
        private String jaasConfig;

        // getters / setters 省略
    }
}
```

### 3.2 新增 `KafkaSecurityAssembler`（核心）

构造期即校验，配置不完整**直接启动失败**，不会带着半套安全参数去连 broker 然后抛看不懂的握手异常。

```java
package com.example.kafkaloader.config;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Component
public class KafkaSecurityAssembler {

    private static final Logger log = LoggerFactory.getLogger(KafkaSecurityAssembler.class);

    private static final Set<String> PROTOCOLS =
            new HashSet<>(Arrays.asList("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"));
    private static final String PLAIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule";
    private static final String SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";

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

    public Map<String, Object> getClientConfigs() { return clientConfigs; }

    /** 不含任何密码，可直接回显到运维接口与日志 */
    public String getSummary() { return summary; }

    public boolean isPlaintext() { return "PLAINTEXT".equals(protocol); }

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
        KafkaSecurityProperties.Ssl ssl = properties.getSsl();
        String truststore = resolve(ssl.getTruststoreLocation(), "kafka.cluster.security.ssl.truststore-location");
        if (truststore == null) {
            log.warn("未配置 ssl.truststore-location，将使用 JDK 默认信任库 cacerts；"
                    + "broker 若为自签或内部 CA 证书，握手一定失败");
        } else {
            configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
            configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, nvl(ssl.getTruststoreType(), "JKS"));
            if (StringUtils.hasText(ssl.getTruststorePassword())) {
                configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ssl.getTruststorePassword());
            }
        }

        String keystore = resolve(ssl.getKeystoreLocation(), "kafka.cluster.security.ssl.keystore-location");
        if (keystore != null) {
            if ("PEM".equalsIgnoreCase(nvl(ssl.getKeystoreType(), "JKS"))) {
                throw new IllegalStateException("ssl.keystore.type=PEM 需要内联 ssl.keystore.certificate.chain / ssl.keystore.key，"
                        + "不支持按路径读取；请转成 PKCS12：openssl pkcs12 -export -in client.crt -inkey client.key -out client.p12");
            }
            if (!StringUtils.hasText(ssl.getKeystorePassword())) {
                throw new IllegalStateException("已配置 ssl.keystore-location，但 ssl.keystore-password 为空");
            }
            configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
            configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, nvl(ssl.getKeystoreType(), "JKS"));
            configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ssl.getKeystorePassword());
            configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                    StringUtils.hasText(ssl.getKeyPassword()) ? ssl.getKeyPassword() : ssl.getKeystorePassword());
        }

        // 空串表示关闭主机名校验（Kafka 客户端认这个语义）
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
        KafkaSecurityProperties.Sasl sasl = properties.getSasl();
        String mechanism = sasl.getMechanism() == null ? "" : sasl.getMechanism().trim().toUpperCase(Locale.ROOT);
        if (mechanism.isEmpty()) {
            throw new IllegalStateException("security.protocol=" + protocol
                    + " 需要配置 kafka.cluster.security.sasl.mechanism（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / GSSAPI）");
        }
        configs.put(SaslConfigs.SASL_MECHANISM, mechanism);
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaas(mechanism, sasl));
    }

    private String buildJaas(String mechanism, KafkaSecurityProperties.Sasl sasl) {
        if (StringUtils.hasText(sasl.getJaasConfig())) {
            return sasl.getJaasConfig().trim();
        }
        if (!StringUtils.hasText(sasl.getUsername()) || !StringUtils.hasText(sasl.getPassword())) {
            throw new IllegalStateException("SASL 认证需要 kafka.cluster.security.sasl.username / password"
                    + "（或直接提供 sasl.jaas-config）");
        }
        String module;
        if ("PLAIN".equals(mechanism)) {
            module = PLAIN_MODULE;
        } else if (mechanism.startsWith("SCRAM-")) {
            module = SCRAM_MODULE;
        } else if ("GSSAPI".equals(mechanism)) {
            throw new IllegalStateException("GSSAPI/Kerberos 必须显式提供 kafka.cluster.security.sasl.jaas-config"
                    + "（含 principal 与 keytab 路径），无法由用户名口令拼装");
        } else {
            throw new IllegalStateException("不支持的 SASL 机制: " + mechanism);
        }
        return module + " required username=\"" + escape(sasl.getUsername())
                + "\" password=\"" + escape(sasl.getPassword()) + "\";";
    }

    private String describe() {
        StringBuilder sb = new StringBuilder("protocol=").append(protocol);
        if (protocol.startsWith("SASL")) {
            sb.append(" | mechanism=").append(properties.getSasl().getMechanism());
            if (StringUtils.hasText(properties.getSasl().getUsername())) {
                sb.append(" | user=").append(properties.getSasl().getUsername());
            }
            sb.append(StringUtils.hasText(properties.getSasl().getJaasConfig()) ? " | jaas=inline" : " | jaas=builtin");
        }
        if (protocol.contains("SSL")) {
            sb.append(" | truststore=").append(fileName(properties.getSsl().getTruststoreLocation(), "JDK 默认"));
            sb.append(" | keystore=").append(fileName(properties.getSsl().getKeystoreLocation(), "无（单向认证）"));
        }
        return sb.toString();
    }

    /** 相对路径按进程工作目录解析（start.sh 会先 cd 到 dist 根），并校验文件可读 */
    private static String resolve(String raw, String key) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        Path path = Paths.get(raw.trim()).toAbsolutePath().normalize();
        if (!Files.isReadable(path)) {
            throw new IllegalStateException(key + " 指向的文件不存在或不可读: " + path);
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

    /** JAAS 串里引号、反斜杠必须转义，否则密码含特殊字符时解析失败 */
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
```

### 3.3 `KafkaConsumerConfig` 改 1 处

```java
    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory(ClusterDefinitionProvider provider,
                                                                KafkaConsumerProperties properties,
                                                                KafkaSecurityAssembler security) {
        Map<String, Object> configs = new HashMap<>();
        // ... 原有 11 行配置不动 ...
        configs.putAll(security.getClientConfigs());   // ← 新增：security.protocol / ssl.* / sasl.*
        log.info("Kafka 消费初始化 | topic={} | brokers={} | security={}",
                provider.get().getTopic(), provider.getBootstrapServers(), security.getSummary());
        return new RefreshingConsumerFactory(provider::getBootstrapServers, configs);
    }
```

> `RefreshingConsumerFactory` 只是把 `baseConfigs` 原样传给 `KafkaConsumer`，安全参数会自动跟着每次重建的 consumer 走，**无需改动**。

### 3.4 `AdminController` 加 1 行

```java
    status.put("kafkaSecurity", security.getSummary());   // 只回显协议/机制/证书文件名，不含密码
```

### 3.5 `application.yml`

把第二节的 `security:` 段整体插到 `kafka.cluster.default-port` 之后，`bootstrap-servers` 之前，默认 `protocol: PLAINTEXT`。

### 3.6 `dist.xml` 增加证书目录

```xml
        <!-- 证书目录占位：真实证书由运维部署时放入，不随包分发（避免私钥进版本库） -->
        <fileSet>
            <directory>${project.basedir}/src/assembly/certs</directory>
            <outputDirectory>config/certs</outputDirectory>
            <fileMode>0600</fileMode>
        </fileSet>
```

`src/assembly/certs/README.txt` 说明放哪些文件、权限要求。**注意不要**把证书放 `src/main/resources`，否则会被打进 jar。

### 3.7 `start.sh` 支持 `JAVA_OPTS`

当前脚本的透传参数是放在 `-jar` 之后的，只能传 `--xxx` 形式的 Spring 参数，`-D` 会被当成程序参数丢掉。Kerberos 需要 `-Djava.security.krb5.conf`，必须补一段：

```bash
# JVM 参数（Kerberos: -Djava.security.krb5.conf=config/krb5.conf）
JAVA_OPTS="${JAVA_OPTS:-}"
nohup java $JAVA_OPTS -Dfile.encoding=UTF-8 \
    -Dloader.path="$LIB_DIR" ...
```

### 3.8 `.gitignore`

```
config/certs/
*.jks
*.p12
*.key
```

---

## 四、启动期校验矩阵

`KafkaSecurityAssembler` 构造时依次校验，任何一条不满足 → 启动失败并在 `logs/console.out` 打出中文原因：

| 场景 | 结果 |
|---|---|
| `protocol` 不在 4 个枚举内 | 抛异常，打印原值与可选值 |
| `protocol` 含 SASL 但 `mechanism` 为空 | 抛异常 |
| `mechanism` 不在 PLAIN/SCRAM-*/GSSAPI | 抛异常 |
| `mechanism=GSSAPI` 且 `jaas-config` 为空 | 抛异常（提示必须给 keytab） |
| `sasl.username/password` 为空且无 `jaas-config` | 抛异常（提示配置环境变量） |
| `truststore-location` 指向的文件不存在 | 抛异常，打印解析后的绝对路径 |
| `keystore-location` 有值但 `keystore-password` 为空 | 抛异常 |
| `keystore-type=PEM` | 抛异常，给出转 PKCS12 的命令 |
| `truststore-location` 留空 | 仅 WARN，回退 JDK 默认信任库 |
| `protocol=PLAINTEXT` | 只下发 `security.protocol=PLAINTEXT`，行为与现在完全一致 |

---

## 五、部署与验证

### 5.1 证书准备

```bash
# 从 broker 端取出 CA（自签场景），或直接用内部 CA 根证书
openssl s_client -connect 10.100.1.180:9093 -showcerts </dev/null 2>/dev/null \
  | openssl x509 -outform PEM > ca.pem

# 转 JKS 信任库（密码用环境变量，不写进脚本历史）
keytool -importcert -noprompt -alias kafka-ca -file ca.pem \
        -keystore config/certs/kafka.truststore.jks -storetype JKS -storepass "$KAFKA_TRUSTSTORE_PASSWORD"

# 双向认证：客户端证书 → PKCS12 → 直接当 keystore 用
openssl pkcs12 -export -in client.crt -inkey client.key -CAfile ca.pem \
        -out config/certs/kafka.keystore.p12 -name kafka-client -passout env:KAFKA_KEYSTORE_PASSWORD
# yml 里 keystore-type 写 PKCS12
```

文件权限：`chmod 600 config/certs/*`，属主为服务运行用户。

### 5.2 先验证再改代码（省时间的关键）

用 Kafka 自带工具以同参数打通，确认证书/账号/端口都对，再去改 Java：

```properties
# /tmp/client.properties
security.protocol=SASL_SSL
ssl.truststore.location=/opt/dist/config/certs/kafka.truststore.jks
ssl.truststore.password=xxx
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="loader" password="xxx";
```

```bash
kafka-consumer-groups.sh --bootstrap-server 10.100.1.180:9093 \
  --describe --group 330100 --command-config /tmp/client.properties
```

这条能跑通，Java 侧只要参数一致就一定能连上。

### 5.3 服务验证

```bash
./bin/start.sh restart
grep "Kafka 连接安全配置" logs/kafka-file-loader.log
curl -s http://localhost:8080/api/admin/status | grep kafkaSecurity
# "protocol=SASL_SSL | mechanism=SCRAM-SHA-512 | user=loader | jaas=builtin | truststore=kafka.truststore.jks | keystore=无（单向认证）"
```

再观察 1~2 个批次是否正常封盘（`file.chunks.*` 指标增长），确认 ACL 已放行消费权限。

---

## 六、故障对照表

| 现象 | 原因 | 处理 |
|---|---|---|
| `SSL handshake failed` / `unable to find valid certification path` | truststore 缺 CA 或链不全 | 把**完整证书链**导入 truststore，不是只有叶子证书 |
| `No subject alternative names present` / `No name matching ... found` | 用 IP 连，证书只签了域名 | 改用域名；内网可临时 `endpoint-identification-algorithm: ""`（有中间人风险） |
| `Authentication failed: Invalid credentials` | SCRAM 用户没建 / 机制不匹配 | broker 侧 `kafka-configs --alter --add-config 'SCRAM-SHA-512=[password=xxx]' --entity-type users --entity-name loader` |
| `Connection to node -1 terminated during authentication` | 连的还是 9092 明文端口 | `bootstrap-servers` 改成安全监听端口 |
| `TopicAuthorizationException` | 账号无 ACL | 授予 group `READ` + topic `READ/DESCRIBE` |
| `Could not find a 'KafkaClient' entry in JAAS` | 用了 `-Djava.security.auth.login.config` 但文件没配 | 用 `sasl.jaas.config` 内联，别依赖外部 JAAS 文件 |
| `KrbException: Cannot locate default realm` | 没指定 krb5.conf | `JAVA_OPTS=-Djava.security.krb5.conf=config/krb5.conf` |
| 启动报 `ssl.truststore-location 指向的文件不存在` | 相对路径基准不对 | 用绝对路径；或确认从 dist 根启动 |

---

## 七、边界与风险

1. **不可热更新**：安全参数与 broker 地址一样在启动时绑定，换证书/改密码必须重启（现状即如此，符合"改配置 = 重启"的既定约定）。
2. **国密 TLS 不支持**：若现场要求 SM2/SM3/SM4 国密套件，原生 `kafka-clients`（3.1.2）不支持，需要 BouncyCastle Provider + 自定义 `SslEngineFactory`，属于另一档工作量，需单独评估。
3. **TLS 1.3 依赖 JDK 版本**：Java 8 需 8u261+ 才支持 `TLSv1.3`；老版本 JDK 上 `enabled-protocols` 里的 TLSv1.3 会被忽略（不影响 TLSv1.2 连接）。
4. **密码不能出现在日志**：`org.apache.kafka` 级别已压到 `WARN`（DEBUG 会打印包含 JAAS 串的完整配置），本方案额外保证 `getSummary()` 与启动日志都不含密码；排查时**不要**临时把 kafka 日志调到 DEBUG 后贴出来。
5. **多实例共用证书**：同一张客户端证书可被多实例复用；若启用 mTLS 且 broker 侧开启证书与 principal 绑定，需为每个实例单独签发。
6. **JAAS 密码与 yml 文件权限**：若不用环境变量而直接写 yml，`config/application.yml` 也要 `chmod 600`。

---

## 八、落地记录（2026-10-27）

现场材料确认：`330100.tar` 内含 **krb5.conf + user.keytab** → 认证方式为 **Kerberos（GSSAPI）**，无客户端证书（单向或明文 SASL）。

已实现并验证（`mvn test` 14 个安全装配用例通过）：

- `KafkaSecurityProperties` + `KafkaSecurityAssembler`（`com.example.kafkaloader.config`）
- `KafkaConsumerConfig` 注入 `security.getClientConfigs()`；`KafkaLoaderApplication` 注册配置类；`/api/admin/status` 回显脱敏摘要 `kafkaSecurity`
- `application.yml`：`kafka.cluster.security` 段（默认 PLAINTEXT）+ Kerberos 启用模板
- `dist.xml`：分发 `config/kerberos/` 占位目录（`src/assembly/kerberos/README.txt`）
- `start.sh` / `start.bat`：支持 `JAVA_OPTS` 追加 JVM 参数
- `.gitignore`：排除 `*.keytab` / `*.jks` / `*.p12` / `*.key` / `config/certs/` / `config/kerberos/`

实现要点（与初版方案的差异）：

1. **krb5.conf 走 JVM 系统属性**：kafka-clients 3.1.2 的消费端没有 `sasl.kerberos.krb5.conf` 参数（那是 broker 端的），客户端只认 `java.security.krb5.conf`。由 Assembler 在装配期（早于任何 Krb5 登录）调用 `System.setProperty` 设置，并尽力调用 `sun.security.krb5.Config.refresh()`，部署时无需再手动传 `-D`（`JAVA_OPTS` 作为备用手段保留）。
2. **keytab 路径统一正斜杠**：Windows 反斜杠在 JAAS 串里要转义，正斜杠两端平台都接受，从根上绕开转义问题。
3. **GSSAPI 的 JAAS 自动拼装**：只需配置 `sasl.username`(principal) + `sasl.keytab-location`，代码自动生成
   `com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false keyTab="..." principal="...";`；复杂场景仍可用 `sasl.jaas-config` 手写覆盖。

### 现场启用步骤（Kerberos）

1. 解压 `330100.tar`，把 `krb5.conf`、`user.keytab` 放到分发包 `config/kerberos/`，`chmod 600`
2. 查 principal：`ktab -l -k user.keytab`（JDK 自带）或 `klist -kt user.keytab`
   - 现场实际值：**REALM=SYNWAY.COM，principal=330100@SYNWAY.COM，KDC=cmkafka:88**（已写入 application.yml）
3. **部署机加 hosts 映射**（krb5.conf 里 KDC 写的是主机名 `cmkafka`，解析不了会在 Kerberos 登录时抛
   `UnknownHostException: cmkafka`）：
   ```bash
   echo "12.100.1.180 cmkafka" >> /etc/hosts
   ping -c1 cmkafka   # 必须能通
   ```
4. `config/application.yml`：`protocol: SASL_PLAINTEXT`（broker 若开 TLS 则 `SASL_SSL`），放开 `sasl` 段填 principal / keytab / krb5.conf，**`bootstrap-servers` 端口换成 broker 的 SASL 监听端口**
5. `./bin/start.sh restart`，日志检索「Kafka 连接安全配置」；`curl /api/admin/status` 核对 `kafkaSecurity`
6. 观察一批数据正常封盘上传即完成切换

> 本地（开发机）冒烟预期：启动后打印
> `Kafka 连接安全配置 | protocol=SASL_PLAINTEXT | mechanism=GSSAPI | principal=330100@SYNWAY.COM | keytab=user.keytab | krb5=krb5.conf | serviceName=kafka | jaas=auto`，
> 随后因开发机无法解析 `cmkafka`（KDC）而登录失败退出 —— 这属于预期，证明配置链路已通；
> 部署机上完成第 3 步后此报错即消失。

### 待确认 / 已确认的现场事实

- 集群 16 台主机名与 IP（已写入 application.yml，`bootstrap-servers` 用主机名）：
  `cmkafka=12.100.1.180`、`kafka01~07=181~187`、`kafka09=189`、`kafka08=190`、`kafka10=203`、
  `kafka11~15=228~232`
- 主题：`DISTRIBUTE_SQAJXYRXX_TO3301`
- 部署机执行 `sudo ./bin/setup-hosts.sh` 一次性写入全部主机名映射（幂等）

### 排障经验（本次现场踩过的坑，按出现顺序）

| 现象 | 根因 | 修复 |
|---|---|---|
| 16 台 broker 全部 `disconnected` | 明文客户端连 SASL 监听端口 | 引入 GSSAPI 认证 |
| `UnknownHostException: cmkafka` | KDC 主机名解析不了 | `setup-hosts.sh` / /etc/hosts |
| `Clock skew too great (37)` | 服务器时区为 EDT，与 KDC 差 12 小时 | `TZ=Asia/Shanghai` + `ntpdate cmkafka` |
| `Asn1Exception 906` | TGS 回包 UDP 截断 | krb5.conf 加 `udp_preference_limit = 1` |
| `Server not found in Kerberos database (7)` | 用 IP 连接，票据名对不上 | `bootstrap-servers` 改主机名 |
