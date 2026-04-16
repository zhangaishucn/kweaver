# DELETE + INSERT 死锁优化设计

## 问题背景

### 死锁现象

```
Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction
```

发生在 `t_flow_dag_var` 表的 `idx_dag_vars_dag_id` 索引上。

### 根因分析

当前 `CreateDagVars` 和 `refreshDagIndexes` 使用 **DELETE + INSERT** 模式：

```go
// 当前实现（有问题）
DELETE FROM t_flow_dag_var WHERE f_dag_id = ?  // 获取间隙锁
INSERT INTO t_flow_dag_var VALUES ...           // 等待插入意向锁
```

在 REPEATABLE READ 隔离级别下：
1. DELETE 在索引间隙上加间隙锁（Gap Lock）
2. INSERT 需要获取插入意向锁（Insert Intention Lock）
3. 并发时，多个事务各自持有间隙锁，互相等待对方的插入意向锁 → **死锁**

### 受影响的方法

| 方法 | 表 | DELETE + INSERT? | 死锁风险 |
|------|-----|------------------|----------|
| `CreateDagVars` | `t_flow_dag_var` | ✅ | 🔴 **已发生** |
| `refreshDagIndexes` | `t_flow_dag_step`, `t_flow_dag_accessor` | ✅ | 🟡 可能 |
| `DeleteDag` | 多表 | ❌ 仅 DELETE | 🟢 低 |

**结论**：仅 `CreateDagVars` 和 `refreshDagIndexes` 需要优化。

---

## 设计方案

### 方案选择：B1 - 完整 diff + 精确操作

**核心思想**：
- 新建时（`isCreate=true`）：直接 INSERT，跳过查询和删除
- 更新时（`isCreate=false`）：查询现有数据 → diff 计算 → 精确操作

**优点**：
1. 彻底消除 DELETE 全量删除带来的间隙锁
2. 仅操作实际变更的行，最小化锁范围
3. 不依赖唯一索引
4. 新建场景零额外开销

---

## 详细设计

### 1. CreateDagVars 优化

#### 1.1 新增参数

```go
func (d *dag) CreateDagVars(ctx context.Context, dagVars []*DagVarModel, isCreate bool) error
```

#### 1.2 流程图

```
输入: dagVars []*DagVarModel, dagID, isCreate

事务内:
┌─────────────────────────────────────────────────────────────┐
│ if isCreate {                                                │
│   // 快速路径: 直接 INSERT                                    │
│   INSERT INTO t_flow_dag_var VALUES (...)                    │
│ } else {                                                     │
│   // 更新路径: diff + 精确操作                                │
│   1. SELECT f_var_name FROM t_flow_dag_var                   │
│      WHERE f_dag_id = ?                                      │
│      → existingNames                                         │
│                                                              │
│   2. diff 计算:                                               │
│      toInsert = 新数据中 var_name 不在 existingNames         │
│      toUpdate = 新数据中 var_name 在 existingNames           │
│      toDelete = existingNames 中不在新数据 var_name          │
│                                                              │
│   3. 执行操作:                                                │
│      if len(toDelete) > 0:                                   │
│        DELETE WHERE f_dag_id=? AND f_var_name IN (?)         │
│      if len(toInsert) > 0:                                   │
│        INSERT INTO ... VALUES ...                            │
│      if len(toUpdate) > 0:                                   │
│        UPDATE SET ... WHERE f_dag_id=? AND f_var_name=?      │
│ }                                                            │
└─────────────────────────────────────────────────────────────┘
```

#### 1.3 diff 匹配规则

- 唯一标识：`f_var_name`
- 更新判断：`f_default_value`, `f_var_type`, `f_description` 任一不同

---

### 2. refreshDagIndexes 优化

#### 2.1 新增参数

```go
func (d *dag) refreshDagIndexes(ctx context.Context, dag *entity.Dag, isCreate bool) error
```

#### 2.2 流程图

```
输入: dag *entity.Dag, isCreate

事务内:
┌─────────────────────────────────────────────────────────────┐
│ // 处理 t_flow_dag_step 表                                   │
│ if isCreate {                                                │
│   INSERT INTO t_flow_dag_step VALUES (...)                   │
│ } else {                                                     │
│   1. SELECT f_id, f_operator, f_source_id, f_has_datasource  │
│      FROM t_flow_dag_step WHERE f_dag_id = ?                 │
│   2. diff by (f_operator, f_source_id)                       │
│   3. 精确 DELETE/INSERT/UPDATE                               │
│ }                                                            │
│                                                              │
│ // 处理 t_flow_dag_accessor 表                               │
│ if isCreate {                                                │
│   INSERT INTO t_flow_dag_accessor VALUES (...)               │
│ } else {                                                     │
│   1. SELECT f_id, f_accessor_id                             │
│      FROM t_flow_dag_accessor WHERE f_dag_id = ?             │
│   2. diff by f_accessor_id                                   │
│   3. 精确 DELETE/INSERT/UPDATE                               │
│ }                                                            │
└─────────────────────────────────────────────────────────────┘
```

#### 2.3 diff 匹配规则

| 表 | 唯一标识 | 更新判断字段 |
|-----|---------|-------------|
| `t_flow_dag_step` | `(f_operator, f_source_id)` | `f_has_datasource` |
| `t_flow_dag_accessor` | `f_accessor_id` | 无（仅 INSERT/DELETE） |

---

### 3. 辅助数据结构

```go
// existingDagVar 现有变量（用于 diff）
type existingDagVar struct {
    VarName      string
    DefaultValue string
    VarType      string
    Description  string
}

// existingDagStep 现有步骤（用于 diff）
type existingDagStep struct {
    ID           uint64
    Operator     string
    SourceID     string
    HasDatasource bool
}

// existingDagAccessor 现有访问者（用于 diff）
type existingDagAccessor struct {
    ID         uint64
    AccessorID string
}

// diffResult diff 计算结果
type diffResult[T any] struct {
    toInsert []T
    toUpdate []T
    toDelete []any // 存储删除 key，不是完整对象
}
```

---

### 4. 调用方修改

| 调用方 | 方法 | isCreate 值 |
|--------|------|-------------|
| `CreateDag` | `CreateDagVars` | `true` |
| `CreateDag` | `refreshDagIndexes` | `true` |
| `UpdateDag` | `CreateDagVars` | `false` |
| `UpdateDag` | `refreshDagIndexes` | `false` |

---

### 5. 不修改的方法

以下方法仅包含 DELETE 操作，不会产生死锁，无需修改：

- `DeleteDag`
- `BatchDeleteDagWithTransaction`
- `DeleteDagInsByID`
- `DeleteTaskInsByDagInsID`
- 其他清理方法

---

## 实现文件清单

| 文件 | 修改类型 | 说明 |
|------|----------|------|
| `store/rds/dag/dag.go` | 修改 | `CreateDagVars`, `refreshDagIndexes` 实现 |
| `store/rds/dag/diff.go` | 新增 | diff 辅助函数 |
| `store/rds/dag/diff_test.go` | 新增 | diff 单元测试 |

---

## 验收标准

1. **死锁消除**：并发创建 DAG 不再产生死锁
2. **功能正确**：Create/Update DAG 功能与修改前一致
3. **性能不降**：新建场景无额外查询开销
4. **测试覆盖**：diff 逻辑有单元测试覆盖

---

## 失败条件

1. 并发测试仍出现死锁 → 检查是否有遗漏的 DELETE + INSERT 场景
2. 数据不一致 → 检查 diff 逻辑边界条件
3. 性能下降明显 → 考虑批量操作优化

---

## 回滚方案

如果优化引入问题，可快速回滚：
1. 恢复 `CreateDagVars` 为 DELETE + INSERT 模式
2. 恢复 `refreshDagIndexes` 为 DELETE + INSERT 模式
3. 移除 `isCreate` 参数

---

## 附录：InnoDB 锁机制说明

### 间隙锁（Gap Lock）

- 在 REPEATABLE READ 隔离级别下，为了防止幻读
- 锁定索引记录之间的间隙
- 例如：索引有值 [10, 20, 30]，DELETE WHERE id=25 会在 [20, 30] 间隙加锁

### 插入意向锁（Insert Intention Lock）

- INSERT 操作需要获取的锁
- 与间隙锁冲突
- 多个插入意向锁之间不冲突

### 死锁形成

```
事务 A: DELETE (间隙锁) → INSERT (等待插入意向锁)
事务 B: DELETE (间隙锁) → INSERT (等待插入意向锁)
→ A 等待 B 的间隙锁释放，B 等待 A 的间隙锁释放 → 死锁
```
