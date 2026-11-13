# 来源协议

fixtures中run.json声明run1的lane1/lane2，read标题允许@id/1和@id 1:N:0:1。FASTQ允许序列和质量跨多行，以+分隔。每端可拼接多个gzip member，R2顺序与R1不同，两个lane含同名read。GET /files/:name按chunkBytes返回gzip原始流；badTail截掉最后8字节。src/count-bytes只计原始字节，不实现配对。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
