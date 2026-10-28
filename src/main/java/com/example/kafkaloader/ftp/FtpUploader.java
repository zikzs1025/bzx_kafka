package com.example.kafkaloader.ftp;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 文件上传抽象，便于替换为 SFTP / 对象存储。
 */
public interface FtpUploader {

    /**
     * 上传单个文件，失败抛异常由上层重试。
     */
    void upload(Path file) throws IOException;
}
