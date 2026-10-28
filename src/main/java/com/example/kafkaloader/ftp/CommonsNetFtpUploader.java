package com.example.kafkaloader.ftp;

import com.example.kafkaloader.config.FtpProperties;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 基于 commons-net 的 FTP 上传。
 * <p>
 * 稳定性要点：
 * <ul>
 *   <li>先传 {@code .uploading} 临时名，完成后 rename，下游永远读不到半截文件</li>
 *   <li>上传后比对远端字节数与本地一致，才算成功</li>
 *   <li>每次上传独立连接，异常不影响其他文件</li>
 * </ul>
 */
@Service
public class CommonsNetFtpUploader implements FtpUploader {

    private static final Logger log = LoggerFactory.getLogger(CommonsNetFtpUploader.class);

    private final FtpProperties properties;

    public CommonsNetFtpUploader(FtpProperties properties) {
        this.properties = properties;
    }

    @Override
    public void upload(Path file) throws IOException {
        FTPClient client = new FTPClient();
        client.setControlEncoding(properties.getEncoding());
        try {
            client.setConnectTimeout(properties.getConnectTimeoutMs());
            client.connect(properties.getHost(), properties.getPort());
            if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                throw new IOException("FTP 连接被拒绝: " + client.getReplyString());
            }
            client.setSoTimeout(properties.getDataTimeoutMs());
            if (!client.login(properties.getUsername(), properties.getPassword())) {
                throw new IOException("FTP 登录失败: " + client.getReplyString());
            }
            if (!client.setFileType(FTP.BINARY_FILE_TYPE)) {
                throw new IOException("无法设置二进制传输模式: " + client.getReplyString());
            }
            if (properties.isPassiveMode()) {
                client.enterLocalPassiveMode();
            }
            client.setBufferSize(properties.getBufferSize());

            ensureRemoteDir(client, properties.getRemoteDir());

            String remoteName = file.getFileName().toString();
            String tempName = remoteName + ".uploading";
            try (InputStream in = Files.newInputStream(file)) {
                if (!client.storeFile(tempName, in)) {
                    throw new IOException("上传失败: " + client.getReplyString());
                }
            }
            if (!client.rename(tempName, remoteName)) {
                try {
                    client.deleteFile(tempName);
                } catch (IOException ignored) {
                    // 清理失败不影响异常抛出
                }
                throw new IOException("重命名失败: " + client.getReplyString());
            }

            long localSize = Files.size(file);
            if (properties.isVerifySize()) {
                long remoteSize = remoteSize(client, remoteName);
                if (remoteSize != localSize) {
                    throw new IOException("远端大小校验不一致: remote=" + remoteSize + " local=" + localSize);
                }
            } else {
                log.debug("ftp.verify-size=false，跳过远端字节数校验: {}", remoteName);
            }
            // 打印实际工作目录而不是配置值：目录切换失败时文件会落在登录目录，配置值会误导排查
            String actualDir;
            try {
                actualDir = client.printWorkingDirectory();
            } catch (IOException e) {
                actualDir = "未知(printWorkingDirectory 失败: " + e.getMessage() + ")";
            }
            if (properties.isVerifySize()) {
                log.info("FTP 上传成功: {} -> {}/{} ({} bytes)，远端已校验存在", remoteName, actualDir, remoteName, localSize);
            } else {
                log.warn("FTP 上传成功: {} -> {}/{} ({} bytes)，但未做远端大小校验，请确认该目录下确实存在该文件",
                        remoteName, actualDir, remoteName, localSize);
            }
        } finally {
            if (client.isConnected()) {
                try {
                    client.logout();
                } catch (IOException ignored) {
                    // 忽略登出异常
                }
                try {
                    client.disconnect();
                } catch (IOException ignored) {
                    // 忽略断开异常
                }
            }
        }
    }

    private long remoteSize(FTPClient client, String remoteName) throws IOException {
        FTPFile[] files = client.listFiles(remoteName);
        if (files == null || files.length == 0) {
            return -1L;
        }
        return files[0].getSize();
    }

    private void ensureRemoteDir(FTPClient client, String remoteDir) throws IOException {
        if (remoteDir == null || remoteDir.trim().isEmpty() || "/".equals(remoteDir.trim())) {
            return;
        }
        StringBuilder current = new StringBuilder();
        for (String part : remoteDir.split("/")) {
            if (part.isEmpty()) {
                continue;
            }
            current.append('/').append(part);
            if (!client.changeWorkingDirectory(current.toString())) {
                if (!client.makeDirectory(current.toString())) {
                    throw new IOException("无法创建远程目录 " + current + ": " + client.getReplyString());
                }
            }
        }
        if (!client.changeWorkingDirectory(remoteDir)) {
            throw new IOException("无法进入远程目录 " + remoteDir + ": " + client.getReplyString());
        }
    }
}
