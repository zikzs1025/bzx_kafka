Kerberos 认证材料目录（部署时由运维放入，文件权限 0600）：

  krb5.conf    KDC 与 realm 配置，来自现场发放的 330100.tar
  user.keytab  服务账号密钥表，来自现场发放的 330100.tar

启用方法：application.yml 里
  kafka.cluster.security.protocol=SASL_PLAINTEXT（或 SASL_SSL）
  kafka.cluster.security.sasl.mechanism=GSSAPI
  kafka.cluster.security.sasl.username=<principal>
  kafka.cluster.security.sasl.keytab-location=./config/kerberos/user.keytab
  kafka.cluster.security.sasl.krb5-conf=./config/kerberos/krb5.conf

principal 可用以下命令从 keytab 中查询：
  klist -kt user.keytab          (Linux)
  ktab -l -k user.keytab         (JDK 自带)
