[English](CHANGELOG.md) | 简体中文

# 变更日志

本文件记录本项目的重要变更。

## [Unreleased]

### 变更

- **布局拆分。** 共用词汇移到 `gcosid/core`（生成器契约、号段模型、进制/前后缀/日期词汇、雪花
  `Layout`、`Stat`），机器号分配移到 `gcosid/machine`。两者都由根包以类型别名重新导出，已有的
  import 与类型断言照旧；`database/`、`redis/` 直接 import `machine` 取得它们实现的接口。根 25 → 19。

### 新增

- `ToString.Parse`：`Format` 的逆操作，十进制 id 可以读回来。

- 初始脚手架。
