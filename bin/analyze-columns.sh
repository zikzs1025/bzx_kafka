#!/usr/bin/env bash
# =============================================================================
# 列填充率分析：判断输出文件的「满屏 \N」是上游本来就空，还是列映射错位
#
# 用法：./bin/analyze-columns.sh [文件路径] [分析行数，默认 200]
# 示例：./bin/analyze-columns.sh data/file-pending/DISTRIBUTE_20261028170000123.txt
#       ./bin/analyze-columns.sh                      # 自动取 pending 目录最新文件
#
# 输出：每列的 填充率 + 去重示例值（最多 3 个）
#   填充率高且示例像身份证/姓名 → 该列是真实数据位
#   填充率 0                     → 上游该位置恒为空
# =============================================================================
set -uo pipefail

DIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$DIST/kafka-file-loader.jar" ]] || DIST="$DIST/target/dist"

FILE="${1:-}"
LIMIT="${2:-200}"
if [[ -z "$FILE" ]]; then
    FILE="$(ls -1t "$DIST"/data/file-pending/* 2>/dev/null | grep -v '\.writing$' | head -1 || true)"
fi
if [[ -z "$FILE" || ! -f "$FILE" ]]; then
    echo "[ERROR] 未找到数据文件，请手工指定： $0 <文件路径>"
    exit 1
fi

# 字段清单（与 DataSchema 顺序一致）
FIELDS=(
  md_id clue_id clue_src_sys clue_dst_sys capture_time protocol original_protocol
  data_source3 mwbs sensitive_level saved_time remark data_check_id bunit data_set_name
  dzwjwz asjxrybh asjbh zjhm xm xbdm xbmc mzdm mzmc csrq xzzqhdm xzzqhmc
  criminal_time gajgjgdm gajgjmc synchro_time
)

echo "== 文件: $FILE"
echo "== 统计前 $LIMIT 行 =="
TOTAL="$(head -n "$LIMIT" "$FILE" | awk 'NF' | wc -l)"
echo "== 有效行数: $TOTAL"
echo
printf '%-4s %-18s %8s  %s\n' "列" "字段名" "填充率" "示例值（去重前3）"
printf '%-4s %-18s %8s  %s\n' "----" "------------------" "--------" "--------------------------------"

for i in $(seq 1 31); do
    read -r filled samples <<<"$(head -n "$LIMIT" "$FILE" | awk -F'\t' -v c="$i" '
        { if (NF < c) next; v=$c; if (v != "" && v != "\\N") { n++; if (!(v in seen) && shown < 3) { seen[v]=1; s = s (shown ? " | " : "") substr(v,1,28); shown++ } } }
        END { printf "%d\t%s", n+0, s }')"
    rate=0
    [[ "$TOTAL" -gt 0 ]] && rate=$(( filled * 100 / TOTAL ))
    printf '%-4s %-18s %7s%%  %s\n' "$i" "${FIELDS[$((i-1))]:-?}" "$rate" "${samples:-—}"
done

echo
echo "== 结论提示 =="
echo "  · 若有效列只有少数几列（如 4/13/14/15/16）→ 上游报文列序与《字段清单》不一致，需用 file.column-mapping 适配"
echo "  · 若某列本应有值却整列 \\N → 该位置在上游确实为空，或分隔符/列数不匹配"
echo "  · 若填充率都很低且首列是整行原文 → 分隔符没切开（跑 ./bin/inspect-payload.sh 看原始报文）"
