#!/usr/bin/env bash
# =============================================================================
# 报文格式速查：从隔离文件里取一条真实 payload，还原 \t / \n 转义后统计结构
#   - 一条消息里有几行
#   - 每行按 TAB 切分有多少列（目标 31）
#
# 用法：./bin/inspect-payload.sh [隔离文件路径] [行号，默认 2=第一条数据]
# 示例：./bin/inspect-payload.sh data/file-reject/DISTRIBUTE_reject_20261028.txt
#
# 说明：隔离文件里的 payload 把制表符/换行保存为字面量 \t \n（避免撑坏 CSV），
#       本脚本会先还原再分析。
# =============================================================================
set -uo pipefail

DIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$DIST/kafka-file-loader.jar" ]] || DIST="$DIST/target/dist"

REJECT_DIR="$DIST/data/file-reject"
FILE="${1:-}"
if [[ -z "$FILE" ]]; then
    FILE="$(ls -1t "$REJECT_DIR"/*_reject_* 2>/dev/null | head -1 || true)"
fi
if [[ -z "$FILE" || ! -f "$FILE" ]]; then
    echo "[ERROR] 未找到隔离文件，请手工指定： $0 <reject 文件>"
    exit 1
fi

LINE_NO="${2:-2}"
PAYLOAD_RAW="$(awk -F'\t' -v n="$LINE_NO" 'NR==n {print $(NF-1)}' "$FILE")"
REASON="$(awk -F'\t' -v n="$LINE_NO" 'NR==n {print $NF}' "$FILE")"

if [[ -z "$PAYLOAD_RAW" ]]; then
    echo "[ERROR] 第 $LINE_NO 行没有 payload，文件: $FILE"
    exit 1
fi

# 还原字面量 \t \n 为真实控制字符
PAYLOAD="$(printf '%s' "$PAYLOAD_RAW" | sed 's/\\t/\t/g; s/\\n/\n/g')"

echo "== 文件: $FILE（第 $LINE_NO 行）=="
echo "== 隔离原因: ${REASON:-（旧格式无 reason 列）} =="
echo
echo "== 第 1 行原文（TAB=^I，行尾=\$，截断 400 字符）=="
printf '%s\n' "$PAYLOAD" | head -1 | cat -A | cut -c1-400
echo
LINES="$(printf '%s\n' "$PAYLOAD" | wc -l)"
echo "== 结构统计 =="
printf '%-20s %s\n' "消息内行数:" "$LINES"
echo "  每行按 TAB 切分的列数（前 5 行）:"
printf '%s\n' "$PAYLOAD" | head -5 | awk -F'\t' '{printf "    第 %d 行: %d 列\n", NR, NF}'
echo
echo "== 结论建议 =="
FIRST_COLS="$(printf '%s\n' "$PAYLOAD" | head -1 | awk -F'\t' '{print NF}')"
if [[ "$FIRST_COLS" == "31" ]]; then
    echo "TAB 切分正好 31 列 → 默认配置即可（parse-mode: AUTO / delimiter-regex: \"\\\\t\" / strict-field-count: true）"
    [[ "$LINES" -gt 1 ]] && echo "一条消息含 $LINES 行 → 程序会拆成 $LINES 行输出，无需额外配置"
else
    echo "TAB 切分为 $FIRST_COLS 列（目标 31），需要进一步确认："
    echo "  - 若长时间为空列（连续 TAB）很多，属正常（空字段占位）"
    echo "  - 把上面「第 1 行原文」整段发我，可精确判断分隔规则"
fi
