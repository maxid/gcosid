[English](README.md) | 简体中文

# gcosid

Go 的号段与雪花 ID 生成。

> 状态：**实验性脚手架**（v0.0.0-dev）。

## 范围：与 CosID 功能对齐

目标是完整移植 CosID 的能力，而不是只挑一种策略。
由使用方选择生成器；本库绝不会静默混用不同布局。

| CosID 能力 | 状态 |
|---|---|
| 号段生成器：`IdSegment{offset,maxId,sequence,step,fetchTime,ttl}` 算术（CosID 2.15.1） | 完成 |
| 双缓冲 / 链式预取，带有限超时 | 完成 |
| `SegmentAllocator` 移植 + 内存分配器 | 完成 |
| DB 号段分配器：`cosid(name,last_max_id,last_fetch_time)` 先预留再读取（MySQL、Postgres） | 完成 |
| Redis 号段分配器：`INCRBY cosid:{namespace.name}:adder` | 完成 |
| 号段 TTL / 过期废弃、步长调优 | 完成（TTL）/ 计划中（步长） |
| 雪花：布局/纪元/序列号 + 毫秒、秒与 JS-safe 变体 | 完成 |
| 机器号分发：手工指定 id、租约槽位 — 内存、JDBC `cosid_machine`、Redis Lua 脚本（逐字对齐 2.15.1） | 完成 |
| 存储适配：MongoDB、ZooKeeper | 计划中 |
| 时钟回拨策略：严格报错或自旋/睡眠同步器 | 完成 |
| 字符串转换器：2..62 进制、前缀、后缀、日期前缀、分组、转字符串 | 完成 |
| 雪花友好渲染 `timestamp-machine-sequence` | 完成 |
| CosID 基准兼容向量（号段边界、位布局、字符串） | 计划中 |

不会改变的规则：唯一性只在已配置的分配域内承诺。允许存在空洞、
重启浪费与时钟回退错误——
"看起来连续" 从来不是削弱唯一性的理由。

## CosID 兼容契约

基线是 CosID **v2.15.1**。本库用于 Java 与 Go 两个进程**写同一批表与 Redis 键**的部署，
因此下列内容是契约而非实现细节。改动任何一条都会静默破坏混合部署——双方各自
以为独占了一个机器槽位。

**存储**

- 号段计数表 `cosid(name PK, last_max_id, last_fetch_time)`，`name = {namespace}.{name}`。
  预留必须在**同一事务**内完成：`UPDATE`（`last_max_id = last_max_id + ?`）与 `SELECT`
  不能拆成两条自动提交语句，否则两个调用方会读到同一个上界。
- 机器槽位表 `cosid_machine`，主键 `{namespace}.{补零到 8 位的 machine id}`——补零是主键的一部分。
- 首个机器号是 **0**：空表时 `max(machine_id)+1` 为 NULL，CosID 把 NULL 读作 0。
- `revert` 对非稳定实例清空 `instance_id`，对稳定实例保留；`guard` 把影响 0 行判定为租约丢失。

**驱动**

`affected rows` 指**匹配行数**而非变更行数：MySQL Connector/J 默认设置 `CLIENT_FOUND_ROWS`，
Go 驱动默认不设。请用 `database.OpenMySQL` 打开连接池（它同时开启 `parseTime`）；
直接用 `sql.Open("mysql", dsn)` 会让一次心跳看起来像租约丢失。

**Redis**

号段用 `cosid:{namespace.name}:adder`，机器槽位用 `cosid:<namespace>:itc_idx` / `:adder` / `:revert`，
值为 `machineId|lastStamp`。花括号是 Redis Cluster 的 hash tag，不是装饰；机器号脚本逐字取自
`cosid-spring-redis`。

**ID**

`COSID_EPOCH = 1577203200000`；毫秒布局 41/10/12、秒 31/10/22、JS 安全 41/3/9 与 31/3/19；
字符串族 44/20/16，按定宽进制字段渲染且不减 epoch；友好格式
`yyyyMMddHHmmss[SSS]-machine-sequence`。

## 测试

| 命令 | 覆盖范围 | 依赖 |
|---|---|---|
| `go test ./...` | 纯逻辑：号段算术、雪花布局、转换器、方言 SQL | 无 |
| `go test -tags=integration ./...` | 针对真实 MySQL 与 Redis 的存储行为 | Docker，或已提供的环境 |

**规则**：任何依赖数据库或其他中间件的逻辑都由集成测试套件验证。
单元测试绝不用 mock 伪造 SQL——mock 会欣然接受一条在现实中重复分配
id 的语句（正是这样，一个缺失的事务边界曾经溜过去）。

没有 Docker 时，把测试指向已提供的环境：

```bash
GCOSID_TEST_MYSQL_DSN='test:test@tcp(127.0.0.1:3306)/cosid' \
GCOSID_TEST_REDIS_ADDR=127.0.0.1:6379 \
go test -tags=integration ./...
```

镜像默认是 `mysql:8.4.7` 与 `redis:7`，也可以改用镜像站：

```bash
GCOSID_MYSQL_IMAGE=registry.example.com/library/mysql:8.4.7 \
GCOSID_REDIS_IMAGE=registry.example.com/library/redis:7 \
go test -tags=integration ./...
```

## 安装

```bash
go get github.com/maxid/gcosid
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
