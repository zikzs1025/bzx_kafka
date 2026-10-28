#!/usr/bin/env bash
# =============================================================================
# 一键写入 Kafka 集群主机名映射（幂等，重复执行不会重复追加）
#
# 为什么必须配：Kerberos 客户端向 KDC 申领的票据是 kafka/<连接用的名字>@SYNWAY.COM，
# 若 bootstrap-servers 用 IP，会去要 kafka/12.100.1.180@SYNWAY.COM —— KDC 中不存在，
# 报 GSSException: Server not found in Kerberos database (7) - LOOKING_UP_SERVER。
#
# 用法：sudo ./bin/setup-hosts.sh [--dry-run]
# =============================================================================
set -uo pipefail

HOSTS_FILE="/etc/hosts"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

# 序号 IP 主机名
ENTRIES="
12.100.1.180 cmkafka
12.100.1.181 kafka01
12.100.1.182 kafka02
12.100.1.183 kafka03
12.100.1.184 kafka04
12.100.1.185 kafka05
12.100.1.186 kafka06
12.100.1.187 kafka07
12.100.1.189 kafka09
12.100.1.190 kafka08
12.100.1.203 kafka10
12.100.1.228 kafka11
12.100.1.229 kafka12
12.100.1.230 kafka13
12.100.1.231 kafka14
12.100.1.232 kafka15
"

added=0
skipped=0

echo "== 目标文件: $HOSTS_FILE $([[ $DRY_RUN == 1 ]] && echo '(dry-run，不写入)') =="

while read -r ip name; do
    [[ -z "${ip:-}" || -z "${name:-}" ]] && continue

    # 已存在同名条目但 IP 不同 → 报警，不自动改（避免覆盖别人的配置）
    existing_ip="$(awk -v n="$name" '$0 !~ /^#/ {for (i=2; i<=NF; i++) if ($i == n) {print $1; exit}}' "$HOSTS_FILE")"
    if [[ -n "$existing_ip" ]]; then
        if [[ "$existing_ip" == "$ip" ]]; then
            echo "[skip] $name 已正确映射到 $ip"
            skipped=$((skipped + 1))
        else
            echo "[WARN] $name 已映射到 $existing_ip（与期望 $ip 不一致），请人工确认后修改"
        fi
        continue
    fi

    if [[ $DRY_RUN == 1 ]]; then
        echo "[dry] 将追加: $ip $name"
    else
        echo "$ip $name" >> "$HOSTS_FILE"
        echo "[add] $ip $name"
    fi
    added=$((added + 1))
done <<< "$ENTRIES"

echo
echo "== 结果：新增 $added 条，已存在 $skipped 条 =="
if [[ $DRY_RUN == 0 ]]; then
    echo "== 连通性检查（KDC + 前 3 台 broker）=="
    for h in cmkafka kafka01 kafka02; do
        if ping -c 1 -W 2 "$h" >/dev/null 2>&1; then
            echo "[OK] $h"
        else
            echo "[!!] $h 不通（检查网络策略）"
        fi
    done
fi
