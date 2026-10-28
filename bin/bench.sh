#!/usr/bin/env bash
# =============================================================================
# kafka-file-loader 压测脚本
#
# 用法:
#   ./bin/bench.sh                 # 跑完整场景矩阵（约 6~8 分钟）
#   ./bin/bench.sh quick           # 只跑推荐配置 + 关闭 fsync 对照（约 1 分钟）
#   ./bin/bench.sh one --records=100000 --partitions=16 --threads=3 --label=my
#
# 说明:
#   - 压的是「31 字段渲染 + 分片写入 + fsync + 5 万条滚动 + 封盘备份」这段真实链路，
#     不依赖外部 Kafka 与 FTP：消息用 ConsumerRecord 直接构造，FTP 用只统计不传输的桩。
#   - 每个场景跑完会回读全部文件，逐行校验行数与 31 列，任一不符则以退出码 2 结束。
#   - 结果写入 target/perf/run-<label>.json，汇总见 target/perf/summary.json。
# =============================================================================
set -euo pipefail

APP_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_HOME"

JAVA_BIN="${JAVA_HOME:-}/bin/java"
if [[ ! -x "$JAVA_BIN" ]]; then
    if command -v java >/dev/null 2>&1; then
        JAVA_BIN="$(command -v java)"
    else
        echo "[ERROR] 未找到 java，请设置 JAVA_HOME"
        exit 1
    fi
fi

PERF_DIR="target/perf"
LIB_DIR="target/lib"
CP="target/classes;target/test-classes;target/lib/*"

ensure_build() {
    if [[ ! -d target/classes || ! -d target/test-classes ]]; then
        echo "[INFO] 编译主代码与压测程序..."
        mvn -o -B test-compile
    fi
    if [[ ! -d "$LIB_DIR" ]]; then
        echo "[INFO] 从 fat jar 抽取依赖库（离线可用）..."
        if [[ ! -f target/kafka-file-loader.jar ]]; then
            mvn -o -B package -DskipTests
        fi
        python - "$@" <<'PY'
import zipfile, os, sys
jar = 'target/kafka-file-loader.jar'
os.makedirs('target/lib', exist_ok=True)
with zipfile.ZipFile(jar) as z:
    names = [n for n in z.namelist() if n.startswith('BOOT-INF/lib/') and n.endswith('.jar')]
    for n in names:
        with open(os.path.join('target/lib', os.path.basename(n)), 'wb') as f:
            f.write(z.read(n))
print('[INFO] 已抽取 %d 个依赖 jar' % len(names))
PY
    fi
    mkdir -p "$PERF_DIR"
}

# run <参数...>
run() {
    local label=""
    for a in "$@"; do
        case "$a" in
            --label=*) label="${a#--label=}" ;;
        esac
    done
    printf '  %-22s' "$label"
    if "$JAVA_BIN" -Xms512m -Xmx512m -Dfile.encoding=UTF-8 -cp "$CP" \
        com.example.kafkaloader.bench.LoadTestHarness "$@" --keep=false >/dev/null 2>&1; then
        local tps
        tps=$(python -c "import json;print('%.0f'%json.load(open('$PERF_DIR/run-$label.json',encoding='utf-8'))['writeTps'])" 2>/dev/null || echo "?")
        echo " OK   TPS=$tps"
    else
        echo " FAIL（详见 $PERF_DIR/run-$label.json 或重跑该场景查看输出）"
        return 1
    fi
}

summary() {
    if [[ -f bin/summarize_bench.py ]]; then
        echo
        python bin/summarize_bench.py
    else
        echo
        echo "[INFO] 未找到 bin/summarize_bench.py，跳过汇总（可手动查看 run-*.json）"
    fi
}

ensure_build

MODE="${1:-matrix}"
shift || true

case "$MODE" in
    quick)
        echo "=== 快速对照（2 个场景）==="
        run --records=300000 --partitions=16 --threads=3 --label=Q1-fsync-on-300k
        run --records=300000 --partitions=16 --threads=3 --fsync=false --label=Q2-fsync-off-300k
        ;;
    one)
        echo "=== 单场景 ==="
        run "$@"
        ;;
    matrix)
        echo "=== 完整场景矩阵（17 次运行，约 6~8 分钟）==="
        rm -f "$PERF_DIR"/run-*.json
        for r in 1 2 3; do
            run --records=500000  --partitions=1  --threads=1 --label="A$r-p1t1-500k"
            run --records=500000  --partitions=1  --threads=1 --fsync=false --label="B$r-p1t1-nofsync-500k"
            run --records=500000  --partitions=16 --threads=1 --label="C$r-p16t1-500k"
            run --records=500000  --partitions=16 --threads=3 --label="D$r-p16t3-500k"
        done
        run --records=1000000 --partitions=16 --threads=3 --label=E-p16t3-1m
        run --records=200000  --partitions=16 --threads=3 --per-file=5000 --label=F-smallfile-200k
        run --records=500000  --partitions=16 --threads=3 --batch=2000 --label=G-batch2000-500k
        run --records=200000  --partitions=16 --threads=3 --batch=100  --label=H-batch100-200k
        run --records=3000000 --partitions=16 --threads=3 --label=I-p16t3-3m
        ;;
    *)
        echo "用法: $0 [matrix|quick|one] [额外参数...]"
        exit 1
        ;;
esac

# 背压专项：预期失败（抛 BackpressureException），单独跑不算整体失败
if [[ "$MODE" == "matrix" || "$MODE" == "quick" ]]; then
    echo
    echo -n "  背压专项（max-pending=2，预期拦截）  "
    # 注意：不能直接用 `java ... | grep -q`，grep 匹配到即退出会让 java 收到 SIGPIPE，
    # 在 set -o pipefail 下整条管道被判为失败。先落变量再判断。
    BP_OUT="$("$JAVA_BIN" -cp "$CP" com.example.kafkaloader.bench.LoadTestHarness \
        --records=200000 --partitions=16 --threads=3 --per-file=5000 \
        --max-pending=2 --label=J-backpressure --keep=false 2>&1 || true)"
    if printf '%s' "$BP_OUT" | grep -q "BackpressureException"; then
        echo "OK   已按预期抛 BackpressureException"
    else
        echo "WARN 未观察到背压异常，请检查 guard() 逻辑"
    fi
fi

summary
echo
echo "[完成] 报告: docs/benchmark-report.html"
