# 测序交付流的双端配对与运行完整性接收

这是用于内部数据接入的 Node.js 项目。当前提供可独立启动的本地来源、单次原文读取接口以及来源契约测试；浏览器页面和仪器网关使用合成业务资料，不连接真实账户或设备。

## 环境与命令

Node.js 22.22.1 或更新的 22.x。依赖由 package-lock.json 锁定。

```sh
npm ci --ignore-scripts
npm run build
npm test
npm start
```

`npm start` 在标准输出报告来源地址。默认绑定本机回环地址并使用动态端口；可通过 `HOST` 与 `PORT` 指定监听地址和端口，例如 `HOST=127.0.0.1 PORT=8080 npm start`。按 Ctrl+C 关闭来源；动态端口避免与同机其他接入站冲突。`npm run demo` 在同一进程启动来源，读取一次目录后退出，不代表持续接收业务已经完成。`npm run demo:paired` 使用内存分块来源演示双端接收（乱序两端、坏压缩尾、同名 read 跨 lane、重传、冲突隔离、单端缺口与三层完整度判断），状态写在系统临时目录，不连接真实设备。

```sh
docker build -t paired-reads:local .
docker run --rm --network none paired-reads:local
docker run --rm --network none paired-reads:local npm run demo
```

依赖在镜像构建阶段安装，运行阶段不需要公网。完整来源测试和一次目录演示通常数秒即可结束，适合在90秒以内查看真实输出。浏览器在无界面模式执行，页面交互不依赖人工鼠标。

## 目录

- `src/source.mjs`：本地来源生命周期与HTTP入口。
- `src/client.mjs`：目录读取及原始字节获取。
- `src/gzip-members.mjs`：有界内存的多 gzip member 流式切分、CRC32/ISIZE 校验与坏压缩尾抢救。
- `src/fastq.mjs`：流式 FASTQ 解析（多行序列/质量、长度校验、错位重同步、两种端标识）。
- `src/event-store.mjs`：只追加事件账本、幂等键与状态投影（崩溃恢复的事实来源）。
- `src/staging.mjs`：有界内存双端暂存表，超预算溢出为有序磁盘分块。
- `src/receiver.mjs`：领取、暂存、配对编排与查询/完整度判断。
- `src/demo-paired.mjs`：本地分块投递的双端接收演示。
- `fixtures/source.json`：来源初始状态。
- `docs/source-contract.md`：来源协议范围、控制与领域语义。
- `test/source.test.mjs`：实际来源集成测试。
- `test/gzip-members.test.mjs`、`test/fastq.test.mjs`、`test/event-store.test.mjs`、`test/staging.test.mjs`、`test/receiver.test.mjs`：双端接收各层单元与集成测试。

## 双端接收设计

接收流程为：分块领取（Range 续传到持久 spool）→ gzip member 流式切分与校验 → FASTQ 流式解析 → 有界暂存 → 双端配对 → 查询。关键约束：

- **有界内存**：gzip 侧只缓存单个未完成 member 的压缩字节；暂存侧超过 `budgetBytes` 即把有序条目落为磁盘分块（JSONL），配对后用 tombstone 删除。整个运行不会被读进内存，也不按字节位置 zip 配对。
- **身份与隔离**：read 身份是 `(run, lane, readId)`，同名 read 跨 lane 互不覆盖；`@id/1` 与 `@id 1:N:0:1` 两种端标识等价。同端同内容重复=重传；同端不同内容=冲突隔离，保留第一条且不参与第二条配对。
- **可恢复**：每个不可逆事实先写 `events.log` 并按 `(file, ordinal)` 或 `pairKey` 幂等。重开后重放账本重建状态，已处理文件不重放解析，未领取完的文件带 Range 续传，已发布配对不重复。
- **三层判断不混淆**：流结束（字节到齐）、记录都合法（无坏尾/坏记录）、运行完整（清单计数与实际配对一致且无缺口/冲突）分别返回。查询接口包括 `pairs()`、`gaps()`、`badRecords()`、`badTails()`、`conflicts()`、`retransmits()`、`byteProgress()`、`completeness()`。

运行时状态仅用于来源模拟，重启来源会回到 fixtures 中的值。接入业务自己的持久状态请使用 `.local/` 或明确的外部路径。来源管理入口 `/state` 和 `/control` 是本地操作设施，不能作为绕过浏览器页面的业务写入捷径。
