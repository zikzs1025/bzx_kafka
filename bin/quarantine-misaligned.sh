#!/usr/bin/env bash
# =============================================================================
# 暂停 FTP 上传 + 把已产出的文件从 pending 隔离出去
#
# 用途：列映射尚未确认时，防止错位数据被传到 FTP 污染下游数据湖。
#
# 用法：
#   ./bin/quarantine-misaligned.sh            # 预演（dry-run），只打印将做什么
#   ./bin/quarantine-misaligned.sh --apply    # 真正执行：改配置 + 重启 + 移动文件
#
# 做的事：
#   1. 备份 config/application.yml
#   2. 把 ftp.enabled 改为 false（只改这一行）
#   3. 重启服务（start.sh restart），确认不再上传
#   4. 把 data/file-pending/* 移到 data/file-failed/misaligned-<时间戳>/，并写 .reason
#      （backup/ 副本保留不动，作为本地留档）
#
# 恢复上传：把 config/application.yml 的 ftp.enabled 改回 true 后 restart。
# =============================================================================
set -uo pipefail

DIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$DIST/kafka-file-loader.jar" ]] || DIST="$DIST/target/dist"

CFG="$DIST/config/application.yml"
PENDING_DIR="$DIST/data/file-pending"
FAILED_DIR="$DIST/data/file-failed"
APPLY=0
[[ "${1:-}" == "--apply" ]] && APPLY=1
TS="$(date +%Y%m%d%H%M%S)"
QUARANTINE_DIR="$FAILED_DIR/misaligned-$TS"

say() { echo "$@"; }
run() {
    if [[ $APPLY == 1 ]]; then
        eval "$@"
    else
        echo "[dry] $*"
    fi
}

[[ -f "$CFG" ]] || { echo "[ERROR] 缺少配置: $CFG"; exit 1; }

say "== 工作目录: $DIST"
say "== 模式: $([[ $APPLY == 1 ]] && echo '执行(--apply)' || echo '预演(dry-run，加 --apply 才真正执行)')"
say

# ---------- 1. 配置：关掉 FTP 上传 ----------
CURRENT="$(grep -E '^[[:space:]]+enabled:' "$CFG" | grep -A0 -m1 'true\|false' | awk '{print $2}')"
say "== 1) 配置处理"
say "   当前配置行: $(grep -nE '^ftp:|^[[:space:]]+enabled:' "$CFG" | head -4 | tr '\n' ' ')"
if grep -qE '^ftp:[[:space:]]*$' "$CFG" && awk '/^ftp:[[:space:]]*$/{f=1;next} f&&/^[^[:space:]]/{exit} f&&/^[[:space:]]+enabled:/{print;exit}' "$CFG" | grep -q 'false'; then
    say "   ftp.enabled 已经是 false，跳过修改"
else
    run "cp '$CFG' '$CFG.bak-$TS'"
    say "   已备份: $CFG.bak-$TS"
    # 只在 ftp: 段落内把 enabled: true 改成 false，避免误改 server/management 段
    run "awk 'BEGIN{f=0} /^ftp:[[:space:]]*\$/ {f=1} f==1 && /^[^[:space:]]/ && /^ftp:[[:space:]]*\$/ {f=1; print; next} f==1 && /^[^[:space:]]/ {f=0} f==1 && /^[[:space:]]+enabled:[[:space:]]*true/ {sub(/true/,\"false\"); print; next} {print}' '$CFG' > '$CFG.tmp' && mv '$CFG.tmp' '$CFG'"
    say "   已把 ftp.enabled 改为 false"
fi
say

# ---------- 2. 重启服务 ----------
say "== 2) 重启服务使其生效"
run "cd '$DIST' && ./bin/start.sh restart"
say

# ---------- 3. 隔离 pending ----------
say "== 3) 把待上传文件移入隔离目录"
if [[ ! -d "$PENDING_DIR" ]]; then
    say "   $PENDING_DIR 不存在，跳过"
else
    COUNT="$(find "$PENDING_DIR" -maxdepth 1 -type f 2>/dev/null | wc -l)"
    say "   pending 下待处理文件数: $COUNT"
    if [[ "$COUNT" -eq 0 ]]; then
        say "   无文件需要隔离"
    else
        run "mkdir -p '$QUARANTINE_DIR'"
        run "find '$PENDING_DIR' -maxdepth 1 -type f -print0 | xargs -0 -I{} mv {} '$QUARANTINE_DIR'/"
        run "printf '%s\n' '隔离原因: 列映射未确认，输出列序疑与《字段清单》不一致，防止错位数据上传 FTP' '隔离时间: '$TS '原始位置: '$PENDING_DIR > '$QUARANTINE_DIR/.reason'"
        say "   已移入: $QUARANTINE_DIR"
        say "   清单:"
        if [[ $APPLY == 1 ]]; then
            ls -1 "$QUARANTINE_DIR" | head -10 | sed 's/^/     /'
        else
            find "$PENDING_DIR" -maxdepth 1 -type f | head -10 | sed 's/^/     /'
        fi
    fi
fi
say

# ---------- 4. 后续步骤 ----------
say "== 4) 后续步骤 =="
say "   1) 确认列映射（./bin/analyze-columns.sh 看列填充率）"
say "   2) 修正 config/application.yml 的 column-mapping / strict-field-count"
say "   3) 把 ftp.enabled 改回 true 并 restart"
say "   4) 补数：把 Kafka 消费位点回退到出错前的时间点重投（需要时我可提供命令）"
say "   5) FTP 远端如已有错位文件，需人工清理（本脚本不触碰远端）"
