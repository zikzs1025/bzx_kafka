package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * FTP 上传配置。
 */
@ConfigurationProperties(prefix = "ftp")
public class FtpProperties {

    private boolean enabled = true;

    private String host = "127.0.0.1";

    private int port = 21;

    private String username;

    private String password;

    /** 远端接收目录，不存在会自动逐级创建 */
    private String remoteDir = "/kafka/incoming";

    /** 被动模式（穿透防火墙通常需要开启） */
    private boolean passiveMode = true;

    private String encoding = "UTF-8";

    private int connectTimeoutMs = 10_000;

    /** 数据连接超时，大文件上传需要留足 */
    private int dataTimeoutMs = 120_000;

    private int bufferSize = 1024 * 1024;

    /** 上传完成后回读远端文件大小并与本地比对，一致才算成功（建议保持开启） */
    private boolean verifySize = true;

    /** 上传成功并校验通过后删除本地 pending 文件（本地备份仍保留） */
    private boolean deleteAfterUpload = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRemoteDir() {
        return remoteDir;
    }

    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }

    public boolean isPassiveMode() {
        return passiveMode;
    }

    public void setPassiveMode(boolean passiveMode) {
        this.passiveMode = passiveMode;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getDataTimeoutMs() {
        return dataTimeoutMs;
    }

    public void setDataTimeoutMs(int dataTimeoutMs) {
        this.dataTimeoutMs = dataTimeoutMs;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isVerifySize() {
        return verifySize;
    }

    public void setVerifySize(boolean verifySize) {
        this.verifySize = verifySize;
    }

    public boolean isDeleteAfterUpload() {
        return deleteAfterUpload;
    }
    public void setDeleteAfterUpload(boolean deleteAfterUpload) {
        this.deleteAfterUpload = deleteAfterUpload;
    }
}
