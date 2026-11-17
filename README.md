# 导入撤销与精确补偿系统（Pointwise Delivery）

当用户撤销一次导入时，下游可能已经把它与其他批次合并，**删除一份原文件不等于撤销完成**。
本系统用「事件账本 + 贡献关系图 + 订阅投递幂等层」解决这个问题，提供可操作网页，并保留
原始导入与撤销证据。纯 Python 标准库实现（`sqlite3` + `http.server`），无第三方依赖。

## 它解决什么

| 需求 | 实现手段 |
|---|---|
| 撤销不能只删原文件 | 原始文件永久保留在 `raw/`；撤销是追加反向事件，不是物理删除 |
| 区分完全依赖 / 部分依赖 | `contributions` 贡献图：单一批次喂大的产物为 `FULL`，多批次合并后降级为 `PARTIAL` |
| 区分已外发结果 | 产物 `externalized` 一旦投递即置位；已外发结果只能**通知失效/更正**，无法物理撤回 |
| 精确补偿或重建 | `compensate` 反向 `ADJUSTMENT`；`rebuild` 从剩余原始记录全量重算 |
| 并发撤销与新批次 | 每连接 `BEGIN IMMEDIATE` + 分块事务；处理中的导入撤销返回 409，作业后可重试 |
| 补偿重复发送不重复扣减 | 事件、消息均带全局唯一幂等键；消费者侧 `consumer_acks` 最多 ack 一次 |
| 消费者离线后恢复 | 离线期间投递挂起，上线后整段补发；补发重复消息命中 ack 账本 |
| 最终明细/汇总不重复扣减 | `verify` 用未撤销原始记录独立重算，校验产物值、事件账本、ground truth 三者一致 |

## 正确性边界（页面与报告中均有说明）

- **保证正确**：所有**未外发**产物，撤销后余额 = 事件账本求和 = 剩余原始记录 ground truth。
  - 精确补偿：按贡献图里该导入的实际贡献生成等额反向 `ADJUSTMENT`，增量小。
  - 重建：丢弃受影响产物现值，从全部未撤销原始记录重算，可自愈任何历史漂移，成本是全量扫描。
- **只能通知失效**：**已外发**产物不可能被悄悄撤回。系统将其标记 `invalidated`，并下发
  `product.invalidated` / `aggregate.corrected`；下游是否采纳以消费者 ack 为准。
- **不会发生**：重复撤销、补偿重发、离线补发重放导致的二次扣减——幂等键在事件层和消费者层各挡一道。

## 目录结构

```
app/
  db.py            SQLite schema（事件账本、贡献图、投递表、ack 表、oplog）
  service.py       登记、分块加工、贡献图维护、撤销（补偿/重建）、独立校验
  messaging.py     订阅方、挂起/补发、redrive、消费者幂等 ack、外发标记
  sample_data.py   确定性样例批次（整数分，全局唯一 record_uid）
  demo.py          三个端到端场景：full / concurrency / idempotency
  server.py        标准库 HTTP：REST API + 静态页
  static/          index.html / app.js / styles.css（无构建步骤）
tests/
  test_system.py   26 项后端集成测试（真实 SQLite、真实线程、真实文件）
  ui_smoke.js      前端 DOM 渲染与动作接线冒烟测试
run_server.sh / run_demo.sh   以 python -I 启动的入口
```

## 运行

```bash
# 1. 启动网页（默认 http://127.0.0.1:8000/）
./run_server.sh --data-dir runtime/web --port 8000

# 2. 命令行跑三个演示场景，产出 JSON 报告与证据文件
./run_demo.sh --scenario all

# 3. 测试
python3 -I tests/test_system.py
node tests/ui_smoke.js
```

页面操作：登记样例/上传 JSON → 加工（分块事务）→ 新增订阅方（可先离线）→
在导入行点「撤销」查看 FULL/PARTIAL/外发预览并选择策略 → 订阅方上线补发 →
对消息点「重复发送」观察幂等拦截 → 顶部卡片实时显示三方一致性校验。

## 数据模型要点

- **金额一律整数分**，所有聚合都是精确整数运算，无浮点误差。
- `events` 只追加：`ITEM_ADDED / ITEM_REMOVED / ADJUSTMENT / SNAPSHOT / INVALIDATION`，
  `idempotency_key` 全局唯一；`seq` 由触发器从自增 row id 生成，并发下无 `MAX+1` 竞争。
- 任意产物的 `current_amount/current_qty` 必须等于其事件流的代数和，`verify` 强制检查。
- 撤销后原始记录保留且 `active=0`，原文件与撤销报告分别落在 `raw/`、`evidence/revocations/`。

## 证据留存

- 原始导入：`runtime/.../raw/<import_id>__<文件名>`（登记时复制，之后不改不删，含 SHA-256）。
- 撤销报告：`runtime/.../evidence/revocations/<revocation_id>.json`，含影响预览、逐产物调整、
  失效清单和正确性保证说明。
- 审计轨迹：所有写操作进入 `oplog`；每个数值变化都可在 `events` 按幂等键追溯。
