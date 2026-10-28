#!/usr/bin/env bash
# =============================================================================
# 一键启动 / 停止 kafka-file-loader
#
# 用法: ./bin/start.sh [start|stop|restart|status] [额外 java 参数...]
# 例如: ./bin/start.sh start --server.port=18090 --spring.profiles.active=prod
#
# 目录布局（jar / lib / config 分离）：
#   dist/kafka-file-loader.jar      业务代码
#   dist/lib/*.jar                  第三方依赖（靠 -Dloader.path 加载）
#   dist/config/application.yml     配置，改完重启生效
#   dist/config/logback-spring.xml  日志策略
#
# 本脚本既能在分发包根（dist/bin/start.sh）运行，
# 也能在源码根（bin/start.sh）运行 —— 后者自动使用 target/dist。
#
# 日志：
#   logs/kafka-file-loader.log                  当前写入文件
#   logs/kafka-file-loader.yyyy-MM-dd.N.log.gz  按天 + 200MB 切分的归档，保留 15 天
#   logs/console.out                            logback 就绪前的早期输出
#
# 注意：进程 stdout 不能重定向到滚动日志同一个文件，否则文件轮转后 shell 仍写旧句柄，
#       会把已归档的文件继续撑大。因此这里分开。
# =============================================================================
set -euo pipefail

APP_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 分发包布局（jar 与 lib 同级）优先；否则用源码构建产物
if [[ -f "$APP_HOME/kafka-file-loader.jar" && -d "$APP_HOME/lib" ]]; then
    DIST="$APP_HOME"
else
    DIST="$APP_HOME/target/dist"
fi

JAR="$DIST/kafka-file-loader.jar"
LIB_DIR="$DIST/lib"
CONFIG_DIR="$DIST/config"
LOG_DIR="$DIST/logs"
LOG_FILE="$LOG_DIR/kafka-file-loader.log"
CONSOLE_OUT="$LOG_DIR/console.out"
PID_FILE="$DIST/kafka-file-loader.pid"
PORT_FILE="$DIST/kafka-file-loader.port"
ACTION="${1:-start}"
shift || true

# 关键：脚本级参数必须在这里缓存成数组。
# 函数体内的 "$@" 取的是「函数自己的位置参数」，直接写 "$@" 会把
# --server.port=xxx 这类透传参数丢掉（java 收到的是空参数）。
EXTRA_ARGS=("$@")

# ---------------------------------------------------------------------------
# 定位真实 Java 进程 PID
#   nohup ... & 之后的 $! 在部分环境（Git Bash / MSYS）只是外层包装 shell 的 PID，
#   与 JVM 实际 PID 不同，直接写入 pid 文件会导致 stop 杀不掉真正的进程。
#   这里按「进程命令行里包含目标 jar」反查，jps / pgrep 都不可用时才回退 $!。
# ---------------------------------------------------------------------------
# 进程存活判断
#   注意：MSYS / Git Bash 的 kill -0 只认自己派生过的进程，对原生 Windows PID
#   会直接返回「不存在」，导致 stop 假成功。因此优先用 jps 按 PID 精确核对。
pid_alive() {
    local pid="${1:-}"
    [[ -z "$pid" ]] && return 1
    if command -v jps >/dev/null 2>&1; then
        jps -l 2>/dev/null | awk '{print $1}' | grep -qx "$pid" && return 0
    fi
    kill -0 "$pid" 2>/dev/null
}

# 发送终止信号：Windows 走 taskkill，其它平台走 kill（SIGTERM 优雅停机）
signal_pid() {
    local pid="$1" force="${2:-0}"
    if command -v taskkill >/dev/null 2>&1; then
        # Windows 控制台进程无法接收 SIGTERM，只能强制结束。
        # Git Bash / Cygwin 会把 /F 当成路径做转换，必须写成 //F //PID。
        case "${OSTYPE:-}" in
            msys*|cygwin*|win32*) taskkill //F //PID "$pid" >/dev/null 2>&1 || true ;;
            *)                    taskkill /F /PID "$pid" >/dev/null 2>&1 || true ;;
        esac
    elif [[ "$force" == "1" ]]; then
        kill -9 "$pid" 2>/dev/null || true
    else
        kill "$pid" 2>/dev/null || true
    fi
}

resolve_pid() {
    local p=""
    if command -v jps >/dev/null 2>&1; then
        p="$(jps -l 2>/dev/null | awk -v jar="$(basename "$JAR")" 'index($0, jar) > 0 {print $1; exit}')"
        [[ -n "$p" ]] && { echo "$p"; return 0; }
    fi
    if command -v pgrep >/dev/null 2>&1; then
        p="$(pgrep -f "$(basename "$JAR")" 2>/dev/null | head -1 || true)"
        [[ -n "$p" ]] && { echo "$p"; return 0; }
    fi
    echo ""
}

current_pid() {
    local p=""
    if [[ -f "$PID_FILE" ]]; then
        p="$(cat "$PID_FILE" 2>/dev/null || true)"
        # pid 文件里的号可能已被复用或本身就不可靠，先按存活校验
        if [[ -n "$p" ]] && pid_alive "$p"; then
            echo "$p"
            return 0
        fi
    fi
    p="$(resolve_pid)"
    [[ -n "$p" ]] && { echo "$p"; return 0; }
    echo ""
}

is_running() {
    [[ -n "$(current_pid)" ]]
}

# 解析实际生效的 http 端口：命令行 > config/application.yml > 8080
resolve_port() {
    local p="" a
    for a in ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}; do
        case "$a" in
            --server.port=*) p="${a#*=}" ;;
        esac
    done
    # 未显式指定时优先用启动时记录的实际端口（status 子命令用）
    if [[ -z "$p" && -f "$PORT_FILE" ]]; then
        p="$(cat "$PORT_FILE" 2>/dev/null || true)"
    fi
    if [[ -z "$p" && -f "$CONFIG_DIR/application.yml" ]]; then
        p="$(awk '
            /^server:[[:space:]]*$/ { inblock = 1; next }
            inblock && /^[^[:space:]#]/ { inblock = 0 }
            inblock && /^[[:space:]]+port:[[:space:]]*[0-9]+/ {
                gsub(/[^0-9]/, "", $0); print; exit
            }
        ' "$CONFIG_DIR/application.yml")"
    fi
    echo "${p:-8080}"
}

do_build_if_needed() {
    if [[ -f "$JAR" && -d "$LIB_DIR" ]]; then
        return
    fi
    echo "[INFO] 未发现分发包，开始构建（mvn clean package -DskipTests）..."
    command -v mvn >/dev/null 2>&1 || { echo "[ERROR] 未找到 mvn，无法构建"; exit 1; }
    ( cd "$APP_HOME" && mvn -B clean package -DskipTests )
    DIST="$APP_HOME/target/dist"
    JAR="$DIST/kafka-file-loader.jar"
    LIB_DIR="$DIST/lib"
    CONFIG_DIR="$DIST/config"
    LOG_DIR="$DIST/logs"
    LOG_FILE="$LOG_DIR/kafka-file-loader.log"
    CONSOLE_OUT="$LOG_DIR/console.out"
    PID_FILE="$DIST/kafka-file-loader.pid"
}

do_start() {
    command -v java >/dev/null 2>&1 || { echo "[ERROR] 未找到 java，请先安装 JDK 8"; exit 1; }
    do_build_if_needed

    [[ -f "$JAR" ]] || { echo "[ERROR] 缺少 jar: $JAR"; exit 1; }
    [[ -d "$LIB_DIR" ]] || { echo "[ERROR] 缺少依赖目录: $LIB_DIR（jar 与 lib 必须分离部署）"; exit 1; }
    [[ -f "$CONFIG_DIR/application.yml" ]] || {
        echo "[ERROR] 缺少配置: $CONFIG_DIR/application.yml"; exit 1; }

    if is_running; then
        echo "[WARN] 服务已在运行，PID=$(current_pid)"
        exit 0
    fi

    mkdir -p "$LOG_DIR"
    cd "$DIST"
    rm -f "$PID_FILE" "$PORT_FILE"

    if [[ -z "${FTP_PASSWORD:-}" ]]; then
        echo "[WARN] 未设置 FTP_PASSWORD，将使用 config/application.yml 中的默认密码"
    fi

    # -Dloader.path：告诉 PropertiesLauncher 去 lib/ 找第三方依赖（漏了会 ClassNotFound）
    # -Dlogging.config：日志策略从外部 config/ 目录加载
    # JAVA_OPTS：允许追加 JVM 参数（如 Kerberos 的 -Djava.security.krb5.conf=config/kerberos/krb5.conf）；
    # 透传参数在 -jar 之后只能传 --xxx 形式的 Spring 参数，-D 必须走这里
    nohup java ${JAVA_OPTS:-} -Dfile.encoding=UTF-8 \
        -Dloader.path="$LIB_DIR" \
        -Dlogging.config="$CONFIG_DIR/logback-spring.xml" \
        -jar "$JAR" \
        --logging.file.path="$LOG_DIR" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} >>"$CONSOLE_OUT" 2>&1 &

    local launched=$!
    sleep 3

    # $! 可能是包装 shell，优先反查真实 JVM 进程
    local pid
    pid="$(resolve_pid)"
    [[ -z "$pid" ]] && pid="$launched"
    echo "$pid" >"$PID_FILE"

    local port
    port="$(resolve_port)"
    echo "$port" >"$PORT_FILE"

    if is_running; then
        echo "[OK] 已启动，PID=$(current_pid)"
        echo "     目录    : $DIST"
        echo "     Swagger : http://localhost:$port/swagger-ui.html"
        echo "     状态    : http://localhost:$port/api/admin/status"
        echo "     配置    : $CONFIG_DIR/application.yml（改完执行 restart 生效）"
        echo "     日志    : $LOG_FILE"
    else
        echo "[ERROR] 启动失败，请查看:"
        echo "        $CONSOLE_OUT"
        tail -n 50 "$CONSOLE_OUT" 2>/dev/null || true
        exit 1
    fi
}

do_stop() {
    local pid
    pid="$(current_pid)"
    if [[ -z "$pid" ]]; then
        echo "[INFO] 服务未运行"
        rm -f "$PID_FILE" "$PORT_FILE"
        return
    fi

    local graceful=1
    if command -v taskkill >/dev/null 2>&1; then
        # Windows 控制台进程无法接收 SIGTERM，只能强制结束
        graceful=0
        echo "[INFO] 正在停止 PID=$pid（Windows 强制结束）..."
    else
        echo "[INFO] 正在停止 PID=$pid（优雅停机，最多 60s）..."
    fi

    signal_pid "$pid" 0
    local i
    for i in $(seq 1 60); do
        pid_alive "$pid" || break
        sleep 1
    done

    if pid_alive "$pid"; then
        if [[ "$graceful" == "1" ]]; then
            echo "[WARN] 优雅停机超时，强制结束（未提交的 offset 会在下次启动重投）"
        fi
        signal_pid "$pid" 1
        sleep 2
    fi

    if pid_alive "$pid"; then
        echo "[ERROR] PID=$pid 仍在运行，请手动结束该进程"
        exit 1
    fi

    rm -f "$PID_FILE" "$PORT_FILE"
    echo "[OK] 已停止"
}

case "$ACTION" in
    start)   do_start ;;
    stop)    do_stop ;;
    restart) do_stop; do_start ;;
    status)
        if is_running; then
            echo "[RUNNING] PID=$(current_pid)  目录=$DIST  端口=$(resolve_port)"
        else
            echo "[STOPPED] 目录=$DIST"
        fi
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status} [额外 java 参数...]"
        exit 1
        ;;
esac
