# 来源协议

fixtures中run.json声明run1的lane1/lane2，read标题允许@id/1和@id 1:N:0:1。FASTQ允许序列和质量跨多行，以+分隔。每端可拼接多个gzip member，R2顺序与R1不同，两个lane含同名read。GET /files/:name按chunkBytes返回gzip原始流；badTail截掉最后8字节。src/count-bytes只计原始字节，不实现配对。

## 双端接收（src/gzip-members.mjs、src/fastq.mjs、src/event-store.mjs、src/staging.mjs、src/receiver.mjs）

接收侧在不做生物学分析的前提下回答“每个运行、lane、read 的两端是否真的到齐”：

- gzip member 读取器按 RFC1952 线性切分多 member 流，逐 member 校验 CRC32/ISIZE；截断的最后一个 member 产出 `truncated` 事件并抢救完整输出块，与“流正常结束”严格区分。内存上限为单个未完成 member 的压缩字节。
- FASTQ 解析器接受序列与质量换行，质量长度必须等于序列长度；结构错误产出带解压字节区间的坏记录事件，并以下一个 `@` 标题重新同步，不让一条坏记录使后续行永久错位。
- read 身份以 (run, lane, readId) 为键，两种端标识写法（`@id/1`、`@id 1:N:0:1`）等价核对；标题端标识与文件所属端不一致按坏记录隔离。
- 同身份同端相同内容记为重传（retransmit），同身份同端不同内容记为冲突（conflict）并隔离，绝不任选第一条。
- 暂存表（SpillableStaging）内存超预算时把有序条目追加为磁盘 JSONL 分块，配对发布后以 tombstone 逻辑删除；两端到达可相差任意距离，不把整个运行读进内存，也不按位置 zip 配对。
- 只追加事件账本 events.log 记录字节领取（claim）、流结束（stream-done）、坏尾、坏记录、到达、重传、冲突与配对发布；事件按 (file, ordinal) 或 pairKey 幂等。中断后重放账本并对 spool 带 Range 续传，已处理文件不重放解析，已发布配对不重复。
- 三层判断分开给出：`streamsDone`（传输层字节到齐）、`recordsValid`（无坏尾与坏记录）、`complete`（清单声明的每 lane 每端计数与实际配对一致且无缺口/冲突）。

## 通用控制

`GET /inventory` 返回本来源当前目录，`GET /health` 检查存活，`GET /state` 查看来源状态。`POST /control` 接收JSON对象，仅在来源支持的字段范围内操作。每次创建服务重新读取 fixtures/source.json；控制用于复现来源演进，不是采集业务的实现接口。

服务使用随机本地端口，测试启动与关闭均由代码负责。没有真实个人资料、凭据或付费调用。
