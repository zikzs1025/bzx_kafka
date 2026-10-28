#!/usr/bin/env bash
# =============================================================================
# Kafka 连接诊断：一条命令定位 disconnected 的根因
# 用法：./bin/diag-kafka.sh [bootstrap端口，默认 9092]
#
# 检查项：
#   1) 当前跑的是否是新包（日志里有没有「Kafka 连接安全配置」行）
#   2) KDC 主机名 cmkafka 能否解析（Kerberos 登录前提）
#   3) broker 端口 TCP 连通性，并顺带探测 9093/9094 是否才是 SASL 端口
#   4) 日志里有没有「terminated during authentication」（说明已到认证阶段，端口是 SASL 的）
# =============================================================================
set -uo pipefail

DIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ ! -f "$DIST/kafka-file-loader.jar" ]]; then
    DIST="$DIST/target/dist"
fi
LOG="$DIST/logs/kafka-file-loader.log"
CFG="$DIST/config/application.yml"
PORT="${1:-9092}"

echo "== 1) 版本与安全配置 =="
if [[ -f "$LOG" ]] && grep -q "Kafka 连接安全配置" "$LOG"; then
    grep "Kafka 连接安全配置" "$LOG" | tail -1
else
    echo "[!] 日志中无「Kafka 连接安全配置」行 → 跑的还是旧包/旧配置"
    echo "    重新部署: target/kafka-file-loader-1.0.0.tar.gz（认证材料已内置）"
fi

echo
echo "== 2) KDC 主机名 cmkafka =="
if getent hosts cmkafka >/dev/null 2>&1 || ping -c1 -W2 cmkafka >/dev/null 2>&1; then
    echo "[OK] cmkafka 可解析"
else
    echo "[!] cmkafka 解析失败 → Kerberos 登录会抛 UnknownHostException"
    echo '    处理: 执行 ./bin/setup-hosts.sh 一键写入 /etc/hosts'
fi

echo
echo "== 3) broker 主机名解析 + TCP 连通性（探测 $PORT，并顺带探测 9093/9094）=="
# 从 yml 的 bootstrap-servers 提取主机（兼容 YAML 列表与逗号串两种写法）
HOSTS="$(awk '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*-[[:space:]]/ {
        sub(/^[[:space:]]*-[[:space:]]*/, "");
        split($0, a, ":");
        if (a[1] != "") print a[1];
        next
    }
    /bootstrap-servers:/ {
        sub(/.*bootstrap-servers:[[:space:]]*/, "");
        n = split($0, a, ",");
        for (i = 1; i <= n; i++) { split(a[i], b, ":"); if (b[1] != "") print b[1] }
    }
' "$CFG" 2>/dev/null | sed 's/[[:space:]].*//' | sort -u | head -3)"
if [[ -z "$HOSTS" ]]; then
    echo "[!] 未能从 $CFG 解析出 bootstrap-servers"
fi
for h in ${HOSTS}; do
    if getent hosts "$h" >/dev/null 2>&1 || ping -c1 -W2 "$h" >/dev/null 2>&1; then
        echo "[OK] $h 可解析"
    else
        echo "[!!] $h 解析失败 → 执行 ./bin/setup-hosts.sh 写入 /etc/hosts"
        continue
    fi
    for p in "$PORT" 9093 9094; do
        if timeout 3 bash -c "cat </dev/null >/dev/tcp/$h/$p" 2>/dev/null; then
            echo "     [OK] $h:$p 可达"
        else
            echo "     [--] $h:$p 不可达"
        fi
    done
done

echo
echo "== 4) 认证阶段判定 =="
if [[ -f "$LOG" ]] && grep -q "terminated during authentication" "$LOG"; then
    echo "[OK] 日志出现「terminated during authentication」→ 端口就是 SASL 的，已到认证阶段"
    echo "     此时 disconnected 可忽略，重点查 principal/keytab/服务名"
else
    echo "（无认证阶段日志 → 还没走到认证，问题在 TCP 层或监听端口不对）"
fi

echo
echo "== 5) 结论提示 =="
echo "  - 全部不可达          → 跨网段防火墙：申请开通 本机 -> 12.100.1.0/24 的 $PORT 端口"
echo "  - 9092 通且无认证日志  → 9092 多半是 PLAINTEXT 监听，SASL 端口是 9093/9094，改 yml 的 bootstrap-servers"
echo "  - 有认证阶段日志       → 端口对了，查 principal 是否与 keytab/KDC 匹配（ktab -l -k user.keytab）"
