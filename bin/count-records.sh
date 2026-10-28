#!/usr/bin/env bash
# =============================================================================
# 统计写入条数（RAW 模式下「一行 = 一条记录」）
#
# 统计口径：
#   stage     正在写入的分片（*.writing，含已写入但未封盘的行）
#   pending   已封盘待上传
#   backup    本地归档（历史全量，按天目录）
#   failed    上传失败/人工隔离
#   reject    隔离的脏数据（按行留存）
#
# 用法：./bin/count-records.sh [--detail]
# =============================================================================
set -uo pipefail

DIST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$DIST/kafka-file-loader.jar" ]] || DIST="$DIST/target/dist"

DETAIL=0
[[ "${1:-}" == "--detail" ]] && DETAIL=1

count_lines() {
    local dir="$1"
    [[ -d "$dir" ]] || { echo "0 0"; return; }
    local files lines
    files="$(find "$dir" -type f ! -name '*.reason' ! -name '.reason' 2>/dev/null | wc -l)"
    # wc -l 只数换行；最后一行若无换行则少算 1，用 grep -c '' 更稳妥
    lines="$(find "$dir" -type f ! -name '*.reason' ! -name '.reason' -print0 2>/dev/null \
        | xargs -0 -r cat 2>/dev/null | wc -l)"
    echo "$files $lines"
}

total_files=0
total_lines=0

printf '%-10s %10s %14s  %s\n' "目录" "文件数" "记录条数" "路径"
printf '%-10s %10s %14s  %s\n' "----------" "----------" "--------------" "------"

for name in stage pending backup failed reject; do
    dir="$DIST/data/file-$name"
    read -r f l <<<"$(count_lines "$dir")"
    printf '%-10s %10s %14s  %s\n' "$name" "$f" "$l" "$dir"
    total_files=$((total_files + f))
    total_lines=$((total_lines + l))
    if [[ $DETAIL == 1 && -d "$dir" ]]; then
        find "$dir" -type f ! -name '*.reason' ! -name '.reason' -printf '%s\t%p\n' 2>/dev/null \
            | sort -k2 | head -20 | awk -F'\t' '{printf "             %10s 字节  %s\n", $1, $2}'
    fi
done

echo
printf '合计：%s 个文件，%s 条记录\n' "$total_files" "$total_lines"
echo
echo "说明："
echo "  · stage 里的 *.writing 是尚未封盘的分片，重启后会转为 pending，不丢数据"
echo "  · 若 stage 行数持续增长但 pending 不增，说明未达到 5 万行/文件且未触发空闲封盘（默认 60s）"
echo "  · 想看某个文件行数：wc -l data/file-pending/xxx.txt"
