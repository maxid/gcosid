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
| 字符串视图：`StringSegmentId`、`StringSnowflakeId` | 完成 |
| 时钟回拨装饰器（任意生成器） | 完成 |
| 本地机器状态文件缓存 | 完成 |
| 主机地址供应商 / `InstanceId.of(host, port)` | 完成 |
| 分片值转换器（标准时间与雪花 ID → 时间） | 完成 |
| 存储适配：MongoDB、ZooKeeper | 计划中（最低优先级） |
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

## ID 注入

CosID 用 `@CosId` 注解，并靠 Spring/MyBatis/Jackson 插件在写库前自动填充。Go 既没有注解，
也没有这些插件点，所以 gcosid 用 **struct tag + 显式调用**：

```go
type Order struct {
    ID     int64 `cosid:"order"`   // 生成器名
    Amount int64
}
```

### 接线方式

```go
// 1. 每种实体一个生成器
chain, err := gcosid.NewChainGenerator("order", 100, alloc)
if err != nil {
    return err
}
defer chain.Close(ctx)

// 2. 注册表：tag 名 → 生成器
injector := gcosid.NewInjector()
injector.Register("order", chain)           // 数值字段
injector.RegisterString("orderNo", orderNo) // 字符串字段

// 3. 仓储是调用点
type OrderRepo struct {
    ids *gcosid.Injector
    db  *sql.DB
}

func (r *OrderRepo) Create(ctx context.Context, order *Order) error {
    if _, err := r.ids.Ensure(ctx, order); err != nil {
        return fmt.Errorf("assign id: %w", err)
    }
    // ... INSERT ...
}

func (r *OrderRepo) CreateBatch(ctx context.Context, orders []*Order) error {
    if _, err := r.ids.EnsureAll(ctx, orders); err != nil {
        return fmt.Errorf("assign ids: %w", err)
    }
    // ... 批量 INSERT ...
}
```

可运行示例见 [`examples/repository`](examples/repository/main.go)。

### 没有自动填充

因为没有框架替你在背后填 id，**跳过 `Ensure` 的写入会插入 0 值主键**。这是唯一需要设计规避的失败模式：

- 在仓储的 create 路径调用 `Ensure`，不要散落在各个 handler 里；
- `Ensure` 幂等——已有 id 的实体**永不覆盖**，服务层与仓储层各调一次也安全；
- `EnsureAll` 批量填充并返回实际填充数量；
- 批量插入必须**先** `EnsureAll` 再拼语句，否则每一行都是 0，主键冲突。

### 规则（对齐 `CosIdAccessor`）

- tag 值是生成器名；`cosid:"-"` 跳过；`cosid:""` 表示共享生成器（`Provider.SetShare`）；
- 只有 `string` 与各宽度整数能承载 id（CosID 是 `String`/`Long`/`Integer`），其余类型在首次 `Ensure` 就报错；
- 目标必须是非空指针，指向 struct；
- 会搜索嵌入 struct，复合实体可继承 id；
- 数值生成器喂字符串字段时按十进制渲染（`ToStringIdConverter`）；
- 字段计划按 `reflect.Type` 缓存，热路径不重复反射。

### 测试替身

`NewInjectorFromProvider(provider)` 每次填充都读注册表，测试可热替换生成器
（`provider.Set("order", fixed)`），不必重建仓储。

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
