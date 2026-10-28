package com.example.kafkaloader.ftp;

import com.example.kafkaloader.config.FtpProperties;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.filesystem.WindowsFakeFileSystem;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用真实 FTP 协议服务端（MockFtpServer，走 loopback TCP）验证 {@link CommonsNetFtpUploader}：
 * 登录 -> 建目录 -> 临时名上传 -> rename -> 回读大小校验。
 * <p>
 * 校验方式刻意用「另开一个 FTP 客户端回读远端列表」而不是读 MockFtpServer 的内部对象，
 * 这样断言走的是协议层，不依赖该库的虚拟文件系统实现细节。
 */
class CommonsNetFtpUploaderTest {

    private static final String REMOTE_DIR = "/ftphome/incoming";

    private FakeFtpServer ftpServer;
    private FtpProperties properties;
    private Path localFile;

    private boolean windows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    @BeforeEach
    void setUp() throws Exception {
        String home = windows() ? "C:\\ftphome" : "/ftphome";

        FileSystem fileSystem = windows() ? new WindowsFakeFileSystem() : new UnixFakeFileSystem();
        if (windows()) {
            fileSystem.add(new DirectoryEntry("C:\\"));
            fileSystem.add(new DirectoryEntry("C:\\ftphome"));
            fileSystem.add(new DirectoryEntry("C:\\ftphome\\incoming"));
        } else {
            fileSystem.add(new DirectoryEntry("/"));
            fileSystem.add(new DirectoryEntry("/ftphome"));
            fileSystem.add(new DirectoryEntry(REMOTE_DIR));
        }

        ftpServer = new FakeFtpServer();
        ftpServer.setServerControlPort(0);
        ftpServer.setFileSystem(fileSystem);
        ftpServer.addUserAccount(new UserAccount("kafka", "kafka", home));
        ftpServer.start();

        properties = new FtpProperties();
        properties.setEnabled(true);
        properties.setHost("127.0.0.1");
        properties.setPort(ftpServer.getServerControlPort());
        properties.setUsername("kafka");
        properties.setPassword("kafka");
        properties.setRemoteDir(REMOTE_DIR);
        properties.setPassiveMode(true);
        properties.setEncoding("UTF-8");
        properties.setVerifySize(true);
        properties.setConnectTimeoutMs(3000);
        properties.setDataTimeoutMs(10000);

        localFile = Files.createTempFile("ftp-upload-", ".csv");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            body.append(i).append("\t张三\t110101\t2026-10-23\tv").append(i).append('\n');
        }
        Files.write(localFile, body.toString().getBytes(StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (ftpServer != null) {
            ftpServer.stop();
        }
        if (localFile != null) {
            Files.deleteIfExists(localFile);
        }
    }

    @Test
    void uploadShouldPlaceFileWithFinalNameAndExactBytes() throws Exception {
        assertDoesNotThrow(() -> new CommonsNetFtpUploader(properties).upload(localFile),
                "上传应成功（内部已包含远端大小校验）");

        String name = localFile.getFileName().toString();
        long localSize = Files.size(localFile);

        List<String> remoteNames = new ArrayList<>();
        long remoteSize = -1;
        FTPClient client = new FTPClient();
        try {
            client.connect("127.0.0.1", ftpServer.getServerControlPort());
            client.login("kafka", "kafka");
            client.setFileType(FTP.BINARY_FILE_TYPE);
            client.enterLocalPassiveMode();
            assertTrue(client.changeWorkingDirectory(REMOTE_DIR), "应能进入远端目录 " + REMOTE_DIR);
            for (FTPFile file : client.listFiles()) {
                remoteNames.add(file.getName());
                if (name.equals(file.getName())) {
                    remoteSize = file.getSize();
                }
            }
        } finally {
            if (client.isConnected()) {
                client.disconnect();
            }
        }

        assertTrue(remoteNames.contains(name), "远端应存在最终文件名，实际列表: " + remoteNames);
        assertEquals(localSize, remoteSize, "远端字节数应与本地完全一致");
        assertFalse(remoteNames.stream().anyMatch(n -> n.endsWith(".uploading")),
                "不应残留 .uploading 临时文件，实际列表: " + remoteNames);
    }

    @Test
    void uploadShouldFailFastWhenServerUnreachable() {
        properties.setPort(1);
        properties.setConnectTimeoutMs(500);
        CommonsNetFtpUploader uploader = new CommonsNetFtpUploader(properties);
        assertThrows(Exception.class, () -> uploader.upload(localFile));
    }
}
