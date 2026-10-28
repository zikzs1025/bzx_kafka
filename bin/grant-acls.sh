#!/usr/bin/env bash
# =============================================================================
# 为 Kerberos 消费账号开通 Kafka ACL（在 broker 机器上以 Kafka 管理员身份执行）
#
# 背景：Kerberos 认证通过后，broker 还要求 ACL 授权，否则报
#   GroupAuthorizationException: Not authorized to access group: <group>
#   TopicAuthorizationException: Not authorized to access topics: [<topic>]
#
# 用法：./grant-acls.sh <BOOTSTRAP> <ADMIN_CONFIG> <PRINCIPAL> <TOPIC> <GROUP>
# 示例：./grant-acls.sh cmkafka:9092 /path/admin.properties 330100 DISTRIBUTE_SQAJXYRXX_TO3301 kafka-hive-loader
#
# ADMIN_CONFIG 是有超级权限的 client.properties，内容示例：
#   security.protocol=SASL_PLAINTEXT
#   sasl.mechanism=GSSAPI
#   sasl.kerberos.service.name=kafka
#   sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/path/admin.keytab" principal="admin@SYNWAY.COM" storeKey=true;
# =============================================================================
set -uo pipefail

BOOTSTRAP="${1:?用法: $0 <BOOTSTRAP> <ADMIN_CONFIG> <PRINCIPAL> <TOPIC> <GROUP>}"
ADMIN_CONFIG="${2:?缺少 ADMIN_CONFIG}"
PRINCIPAL="${3:?缺少 PRINCIPAL，如 330100}"
TOPIC="${4:?缺少 TOPIC}"
GROUP="${5:?缺少 GROUP}"

KAFKA_ACLS="$(command -v kafka-acls.sh || echo "$KAFKA_HOME/bin/kafka-acls.sh")"
if [[ ! -x "$KAFKA_ACLS" ]]; then
    echo "[ERROR] 找不到 kafka-acls.sh，请设置 KAFKA_HOME 或把 Kafka bin 加入 PATH"
    exit 1
fi

run() {
    echo "+ $*"
    "$@"
}

# 1) 主题读权限（含 DESCRIBE）
run "$KAFKA_ACLS" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
    --add --allow-principal "User:${PRINCIPAL}" \
    --operation Read --operation Describe --topic "$TOPIC"

# 2) 消费组读权限（含 DESCRIBE）
run "$KAFKA_ACLS" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
    --add --allow-principal "User:${PRINCIPAL}" \
    --operation Read --operation Describe --group "$GROUP"

# 3) 核对结果
echo
echo "== 当前 ACL =="
run "$KAFKA_ACLS" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --list

echo
echo "== 完成。重启应用验证： ./bin/start.sh restart =="
