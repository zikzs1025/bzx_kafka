@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0.."
set "APP_HOME=%CD%"

echo ============================================
echo   kafka-file-loader 一键启动（jar/lib/config 分离）
echo ============================================

where java >nul 2>nul
if errorlevel 1 (
    echo [ERROR] 未找到 java，请先安装 JDK 8 并配置 PATH
    exit /b 1
)

rem 分发包布局（jar 与 lib 同级）优先；否则用源码构建产物 target\dist
set "DIST=%APP_HOME%"
if not exist "%DIST%\kafka-file-loader.jar" set "DIST=%APP_HOME%\target\dist"
if not exist "%DIST%\lib" set "DIST=%APP_HOME%\target\dist"

if not exist "%DIST%\kafka-file-loader.jar" (
    echo [INFO] 未发现分发包，开始构建...
    where mvn >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] 未找到 mvn，请安装 Maven 或先手动执行 mvn clean package
        exit /b 1
    )
    call mvn -B clean package -DskipTests
    if errorlevel 1 (
        echo [ERROR] 构建失败
        exit /b 1
    )
    set "DIST=%APP_HOME%\target\dist"
)

if not exist "%DIST%\lib" (
    echo [ERROR] 缺少依赖目录 %DIST%\lib（jar 与 lib 必须分离部署）
    exit /b 1
)
if not exist "%DIST%\config\application.yml" (
    echo [ERROR] 缺少配置文件 %DIST%\config\application.yml
    exit /b 1
)

if not exist "%DIST%\logs" mkdir "%DIST%\logs"

if "%FTP_PASSWORD%"=="" (
    echo [WARN] 未设置 FTP_PASSWORD，将使用 config\application.yml 中的默认密码
)

rem 端口提示：以 config\application.yml 的 server.port 为基准（命令行 --server.port 优先级更高）
set "PORT=8080"
for /f "tokens=2" %%a in ('findstr /r /c:"^  port: [0-9][0-9]*" "%DIST%\config\application.yml"') do set "PORT=%%a"

echo [INFO] 程序目录 : %DIST%
echo [INFO] 依赖目录 : %DIST%\lib
echo [INFO] 配置文件 : %DIST%\config\application.yml
echo [INFO] Swagger  : http://localhost:%PORT%/swagger-ui.html
echo [INFO] 状态接口 : http://localhost:%PORT%/api/admin/status
echo [INFO] 运行日志 : %DIST%\logs\kafka-file-loader.log
echo [INFO] 归档日志 : %DIST%\logs\kafka-file-loader.*.log.gz（按天压缩，保留 15 天）
echo [INFO] 启动中（关闭本窗口即停止服务）...
echo.

cd /d "%DIST%"

rem -Dloader.path：PropertiesLauncher 靠它从 lib\ 加载第三方依赖（漏了会 ClassNotFound）
rem -Dlogging.config：日志策略从外部 config 加载
rem JAVA_OPTS：可追加 JVM 参数（如 Kerberos 的 -Djava.security.krb5.conf=config\kerberos\krb5.conf）
java %JAVA_OPTS% -Dfile.encoding=UTF-8 ^
     -Dloader.path="%DIST%\lib" ^
     -Dlogging.config="%DIST%\config\logback-spring.xml" ^
     -jar "%DIST%\kafka-file-loader.jar" ^
     --logging.file.path="%DIST%\logs" %*

endlocal
