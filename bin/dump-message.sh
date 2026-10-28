#!/usr/bin/env bash
# =============================================================================
# 取一条 Kafka 原始报文，原样显示分隔符（TAB 显示为 ^I，行尾显示为 $）
# 用于确认上游报文的真实形态：一行为一条记录还是多行？列分隔符是 TAB 还是空格？
#
# 用法：./bin/dump-message.sh <partition> <offset> [client.properties]
# 示例：./bin/dump-message.sh 0 405 /tmp/client.properties
#
# client.properties 至少要能消费该主题，内容示例：
#   security.protocol=SASL_PLAINTEXT
#   sasl.mechanism=GSSAPI
#   sasl.kerberos.service.name=kafka
#   sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false keyTab="/path/user.keytab" principal="330100@SYNWAY.COM";
# =============================================================================
set -uo pipefail

PARTITION="${1:?用法: $0 <partition> <offset> [client.properties]}"
OFFSET="${2:?缺少 offset，可从未隔离文件里取}"
CLIENT_PROPS="${3:-}"

BOOTSTRAP="${BOOTSTRAP:-cmkafka:9092}"
TOPIC="${TOPIC:-DISTRIBUTE_SQAJXYRXX_TO3301}"
OUT="/tmp/kafka-raw-${PARTITION}-${OFFSET}.txt"

CONSUMER="$(command -v kafka-console-consumer.sh || echo "${KAFKA_HOME:-}/bin/kafka-console-consumer.sh")"
if [[ ! -x "$CONSUMER" ]]; then
    echo "[ERROR] 找不到 kafka-console-consumer.sh，请设置 KAFKA_HOME 或把 Kafka bin 加入 PATH"
    exit 1
fi

ARGS=(--bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --partition "$PARTITION" --offset "$OFFSET"
      --max-messages 1 --timeout-ms 15000)
[[ -n "$CLIENT_PROPS" ]] && ARGS+=(--consumer.config "$CLIENT_PROPS")

echo "+ $CONSUMER ${ARGS[*]}"
"$CONSUMER" "${ARGS[@]}" > "$OUT" 2>/tmp/kafka-raw.err
rc=$?
if [[ $rc -ne 0 && ! -s "$OUT" ]]; then
    echo "[ERROR] 取报文失败，错误输出："
    tail -5 /tmp/kafka-raw.err
    exit 1
fi

echo "== 原始文件: $OUT（$(wc -c <"$OUT") 字节，$(wc -l <"$OUT") 个换行）=="
echo
echo "== 前 3 行（TAB=^I，行尾=\$）=="
head -3 "$OUT" | cat -A | cut -c1-400
echo
echo "== 统计 =="
printf '%-24s %s\n' "行数(以换行分隔):" "$(wc -l <"$OUT")"
printf '%-24s %s\n' "TAB 数量:" "$(tr -cd '\t' <"$OUT" | wc -c)"
printf '%-24s %s\n' "每行 TAB 数(前3行):" "$(head -3 "$OUT" | awk -F'\t' '{print NF-1}' | tr '\n' ' ')"
echo
echo "结论对照："
echo "  TAB 数 > 0 且每行 TAB 数=30  → 一条消息一行、31 列 TAB 分隔（当前默认配置即可）"
echo "  行数 > 1 且每行 TAB 数=30    → 一条消息多行，本程序已支持多行拆分为多行输出"
echo "  TAB 数 = 0                   → 不是 TAB 分隔，把上面 head 的输出发我"
