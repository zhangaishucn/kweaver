# DELETE + INSERT 死锁优化实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 CreateDagVars 和 refreshDagIndexes 中的 DELETE + INSERT 死锁问题

**Architecture:** 使用 diff-based 方法替代全量 DELETE + INSERT。新建时直接 INSERT，更新时先查询现有数据、计算差异、再执行精确操作（仅操作变更的行）。

**Tech Stack:** Go, GORM, MariaDB/InnoDB

---

## 文件结构

| 文件 | 类型 | 职责 |
|------|------|------|
| `store/rds/dag/diff.go` | 新增 | diff 辅助函数和数据结构 |
| `store/rds/dag/diff_test.go` | 新增 | diff 单元测试 |
| `store/rds/dag/dag.go` | 修改 | `CreateDagVars`, `refreshDagIndexes`, `CreateDag`, `UpdateDag` |

---

## Task 1: 实现 diff 辅助函数

**Files:**
- Create: `store/rds/dag/diff.go`
- Test: `store/rds/dag/diff_test.go`

- [ ] **Step 1: 编写 diff 数据结构和函数**

```go
// store/rds/dag/diff.go
package dagmodel

// existingDagVar 现有变量（用于 diff）
type existingDagVar struct {
	VarName      string
	DefaultValue string
	VarType      string
	Description  string
}

// existingDagStep 现有步骤（用于 diff）
type existingDagStep struct {
	ID            uint64
	Operator      string
	SourceID      string
	HasDatasource bool
}

// existingDagAccessor 现有访问者（用于 diff）
type existingDagAccessor struct {
	ID         uint64
	AccessorID string
}

// dagVarsDiff diff 计算结果
type dagVarsDiff struct {
	toInsert []*DagVarModel
	toUpdate []*DagVarModel
	toDelete []string // var_name 列表
}

// dagStepsDiff diff 计算结果
type dagStepsDiff struct {
	toInsert []*DagStepModel
	toUpdate []*DagStepModel
	toDelete []uint64 // id 列表
}

// dagAccessorsDiff diff 计算结果
type dagAccessorsDiff struct {
	toInsert []*DagAccessorModel
	toDelete []uint64 // id 列表
}

// diffDagVars 计算变量差异
func diffDagVars(existing []existingDagVar, newVars []*DagVarModel) *dagVarsDiff {
	result := &dagVarsDiff{
		toInsert: make([]*DagVarModel, 0),
		toUpdate: make([]*DagVarModel, 0),
		toDelete: make([]string, 0),
	}

	existingMap := make(map[string]existingDagVar)
	for _, v := range existing {
		existingMap[v.VarName] = v
	}

	newVarNames := make(map[string]bool)
	for _, newVar := range newVars {
		newVarNames[newVar.VarName] = true
		if existing, ok := existingMap[newVar.VarName]; ok {
			if existing.DefaultValue != newVar.DefaultValue ||
				existing.VarType != newVar.VarType ||
				existing.Description != newVar.Description {
				result.toUpdate = append(result.toUpdate, newVar)
			}
		} else {
			result.toInsert = append(result.toInsert, newVar)
		}
	}

	for _, v := range existing {
		if !newVarNames[v.VarName] {
			result.toDelete = append(result.toDelete, v.VarName)
		}
	}

	return result
}

// stepKey 生成步骤的唯一标识
func stepKey(operator, sourceID string) string {
	return operator + ":" + sourceID
}

// diffDagSteps 计算步骤差异
// 注意：此函数会修改 newSteps 中需要 UPDATE 的元素的 ID 字段
func diffDagSteps(existing []existingDagStep, newSteps []*DagStepModel) *dagStepsDiff {
	result := &dagStepsDiff{
		toInsert: make([]*DagStepModel, 0),
		toUpdate: make([]*DagStepModel, 0),
		toDelete: make([]uint64, 0),
	}

	existingMap := make(map[string]existingDagStep)
	for _, s := range existing {
		key := stepKey(s.Operator, s.SourceID)
		existingMap[key] = s
	}

	newKeys := make(map[string]bool)
	for _, newStep := range newSteps {
		key := stepKey(newStep.Operator, newStep.SourceID)
		newKeys[key] = true

		if existing, ok := existingMap[key]; ok {
			if existing.HasDatasource != newStep.HasDatasource {
				newStep.ID = existing.ID
				result.toUpdate = append(result.toUpdate, newStep)
			}
		} else {
			result.toInsert = append(result.toInsert, newStep)
		}
	}

	for _, s := range existing {
		key := stepKey(s.Operator, s.SourceID)
		if !newKeys[key] {
			result.toDelete = append(result.toDelete, s.ID)
		}
	}

	return result
}

// diffDagAccessors 计算访问者差异
func diffDagAccessors(existing []existingDagAccessor, newAccessors []*DagAccessorModel) *dagAccessorsDiff {
	result := &dagAccessorsDiff{
		toInsert: make([]*DagAccessorModel, 0),
		toDelete: make([]uint64, 0),
	}

	existingMap := make(map[string]existingDagAccessor)
	for _, a := range existing {
		existingMap[a.AccessorID] = a
	}

	newAccessorIDs := make(map[string]bool)
	for _, newAcc := range newAccessors {
		newAccessorIDs[newAcc.AccessorID] = true
		if _, ok := existingMap[newAcc.AccessorID]; !ok {
			result.toInsert = append(result.toInsert, newAcc)
		}
	}

	for _, a := range existing {
		if !newAccessorIDs[a.AccessorID] {
			result.toDelete = append(result.toDelete, a.ID)
		}
	}

	return result
}
```

- [ ] **Step 2: 编写 diff 单元测试**

```go
// store/rds/dag/diff_test.go
package dagmodel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiffDagVars(t *testing.T) {
	t.Run("all insert", func(t *testing.T) {
		existing := []existingDagVar{}
		newVars := []*DagVarModel{
			{VarName: "var1", DefaultValue: "val1", VarType: "string"},
			{VarName: "var2", DefaultValue: "val2", VarType: "int"},
		}
		result := diffDagVars(existing, newVars)
		assert.Len(t, result.toInsert, 2)
		assert.Len(t, result.toUpdate, 0)
		assert.Len(t, result.toDelete, 0)
	})

	t.Run("all delete", func(t *testing.T) {
		existing := []existingDagVar{
			{VarName: "var1", DefaultValue: "val1"},
			{VarName: "var2", DefaultValue: "val2"},
		}
		newVars := []*DagVarModel{}
		result := diffDagVars(existing, newVars)
		assert.Len(t, result.toInsert, 0)
		assert.Len(t, result.toUpdate, 0)
		assert.Len(t, result.toDelete, 2)
	})

	t.Run("update needed", func(t *testing.T) {
		existing := []existingDagVar{
			{VarName: "var1", DefaultValue: "old_val", VarType: "string"},
		}
		newVars := []*DagVarModel{
			{VarName: "var1", DefaultValue: "new_val", VarType: "string"},
		}
		result := diffDagVars(existing, newVars)
		assert.Len(t, result.toInsert, 0)
		assert.Len(t, result.toUpdate, 1)
		assert.Len(t, result.toDelete, 0)
	})

	t.Run("no change", func(t *testing.T) {
		existing := []existingDagVar{
			{VarName: "var1", DefaultValue: "val1", VarType: "string", Description: "desc"},
		}
		newVars := []*DagVarModel{
			{VarName: "var1", DefaultValue: "val1", VarType: "string", Description: "desc"},
		}
		result := diffDagVars(existing, newVars)
		assert.Len(t, result.toInsert, 0)
		assert.Len(t, result.toUpdate, 0)
		assert.Len(t, result.toDelete, 0)
	})

	t.Run("mixed operations", func(t *testing.T) {
		existing := []existingDagVar{
			{VarName: "var1", DefaultValue: "val1"},
			{VarName: "var2", DefaultValue: "old_val"},
		}
		newVars := []*DagVarModel{
			{VarName: "var2", DefaultValue: "new_val"},
			{VarName: "var3", DefaultValue: "val3"},
		}
		result := diffDagVars(existing, newVars)
		assert.Len(t, result.toInsert, 1)
		assert.Len(t, result.toUpdate, 1)
		assert.Len(t, result.toDelete, 1)
	})
}

func TestDiffDagSteps(t *testing.T) {
	t.Run("mixed operations", func(t *testing.T) {
		existing := []existingDagStep{
			{ID: 1, Operator: "op1", SourceID: "src1", HasDatasource: false},
			{ID: 2, Operator: "op2", SourceID: "src2", HasDatasource: false},
		}
		newSteps := []*DagStepModel{
			{Operator: "op2", SourceID: "src2", HasDatasource: true},
			{Operator: "op3", SourceID: "src3", HasDatasource: false},
		}
		result := diffDagSteps(existing, newSteps)
		assert.Len(t, result.toInsert, 1)
		assert.Len(t, result.toUpdate, 1)
		assert.Len(t, result.toDelete, 1)
	})
}

func TestDiffDagAccessors(t *testing.T) {
	t.Run("insert and delete", func(t *testing.T) {
		existing := []existingDagAccessor{
			{ID: 1, AccessorID: "acc1"},
			{ID: 2, AccessorID: "acc2"},
		}
		newAccessors := []*DagAccessorModel{
			{AccessorID: "acc2"},
			{AccessorID: "acc3"},
		}
		result := diffDagAccessors(existing, newAccessors)
		assert.Len(t, result.toInsert, 1)
		assert.Len(t, result.toDelete, 1)
	})
}
```

- [ ] **Step 3: 运行测试验证**

Run: `cd /home/zhang/kweaver/kweaver-core/adp/dataflow/flow-automation && go test ./store/rds/dag/... -run TestDiff -v`

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add store/rds/dag/diff.go store/rds/dag/diff_test.go
git commit -m "feat: add diff helper functions for deadlock fix

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: 添加查询辅助方法

**Files:**
- Modify: `store/rds/dag/dag.go`

- [ ] **Step 1: 添加查询现有数据的方法**

在 `dag.go` 中添加以下方法（在 `CreateDagVars` 方法之前）：

```go
// getExistingDagVars 查询现有变量
func (d *dag) getExistingDagVars(ctx context.Context, dagID uint64) ([]existingDagVar, error) {
	db, _, cancel := d.dbWithContext(ctx)
	defer cancel()

	var vars []existingDagVar
	sqlStr := `SELECT f_var_name, f_default_value, f_var_type, f_description FROM t_flow_dag_var WHERE f_dag_id = ?`
	trace.SetAttributes(ctx, attribute.String(trace.TABLE_NAME, DAGVAR_TABLENAME), attribute.String(trace.DB_SQL, sqlStr))
	err := db.Raw(sqlStr, dagID).Scan(&vars).Error
	return vars, err
}

// getExistingDagSteps 查询现有步骤
func (d *dag) getExistingDagSteps(ctx context.Context, dagID uint64) ([]existingDagStep, error) {
	db, _, cancel := d.dbWithContext(ctx)
	defer cancel()

	var steps []existingDagStep
	sqlStr := `SELECT f_id, f_operator, f_source_id, f_has_datasource FROM t_flow_dag_step WHERE f_dag_id = ?`
	trace.SetAttributes(ctx, attribute.String(trace.TABLE_NAME, DAGSTEPINDEX_TABLENAME), attribute.String(trace.DB_SQL, sqlStr))
	err := db.Raw(sqlStr, dagID).Scan(&steps).Error
	return steps, err
}

// getExistingDagAccessors 查询现有访问者
func (d *dag) getExistingDagAccessors(ctx context.Context, dagID uint64) ([]existingDagAccessor, error) {
	db, _, cancel := d.dbWithContext(ctx)
	defer cancel()

	var accessors []existingDagAccessor
	sqlStr := `SELECT f_id, f_accessor_id FROM t_flow_dag_accessor WHERE f_dag_id = ?`
	trace.SetAttributes(ctx, attribute.String(trace.TABLE_NAME, DAGACCESSORINDEX_TABLENAME), attribute.String(trace.DB_SQL, sqlStr))
	err := db.Raw(sqlStr, dagID).Scan(&accessors).Error
	return accessors, err
}
```

- [ ] **Step 2: Commit**

```bash
git add store/rds/dag/dag.go
git commit -m "feat: add query helper methods for diff-based update

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: 优化 CreateDagVars 方法

**Files:**
- Modify: `store/rds/dag/dag.go:412-459`

- [ ] **Step 1: 添加 insertDagVars 辅助方法**

```go
// insertDagVars 插入变量
func (d *dag) insertDagVars(ctx context.Context, dagVars []*DagVarModel) error {
	if len(dagVars) == 0 {
		return nil
	}

	sqlStr := `INSERT INTO t_flow_dag_var (f_id, f_dag_id, f_var_name, f_default_value, f_var_type, f_description) VALUES `
	values := make([]any, 0, len(dagVars)*6)
	for _, data := range dagVars {
		sqlStr += "(?, ?, ?, ?, ?, ?),"
		values = append(values, data.ID, data.DagID, data.VarName, data.DefaultValue, data.VarType, data.Description)
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")

	return d.db.Exec(sqlStr, values...).Error
}
```

- [ ] **Step 2: 重写 CreateDagVars 方法**

替换原有的 `CreateDagVars` 方法（第 412-459 行）：

```go
func (d *dag) CreateDagVars(ctx context.Context, dagVars []*DagVarModel, isCreate bool) error {
	var err error
	newCtx, span := trace.StartInternalSpan(ctx)
	msgStr, _ := jsoniter.MarshalToString(dagVars)
	defer func() { trace.TelemetrySpanEnd(span, err) }()

	fn := func(store *dag, dagVars []*DagVarModel, isCreate bool) error {
		if len(dagVars) == 0 {
			return nil
		}

		dagID := dagVars[0].DagID

		if isCreate {
			// 快速路径：直接 INSERT
			return store.insertDagVars(newCtx, dagVars)
		}

		// 更新路径：diff + 精确操作
		existing, err := store.getExistingDagVars(newCtx, dagID)
		if err != nil {
			return err
		}

		diff := diffDagVars(existing, dagVars)

		// 执行删除（构建正确的 IN 子句）
		if len(diff.toDelete) > 0 {
			placeholders := make([]string, len(diff.toDelete))
			args := make([]any, len(diff.toDelete)+1)
			args[0] = dagID
			for i, name := range diff.toDelete {
				placeholders[i] = "?"
				args[i+1] = name
			}
			sqlStr := fmt.Sprintf("DELETE FROM t_flow_dag_var WHERE f_dag_id = ? AND f_var_name IN (%s)",
				strings.Join(placeholders, ","))
			trace.SetAttributes(newCtx, attribute.String(trace.TABLE_NAME, DAGVAR_TABLENAME), attribute.String(trace.DB_SQL, sqlStr))
			if err = store.db.Exec(sqlStr, args...).Error; err != nil {
				return err
			}
		}

		// 执行插入
		if len(diff.toInsert) > 0 {
			if err = store.insertDagVars(newCtx, diff.toInsert); err != nil {
				return err
			}
		}

		// 执行更新（逐行）
		if len(diff.toUpdate) > 0 {
			for _, v := range diff.toUpdate {
				sqlStr := `UPDATE t_flow_dag_var SET f_default_value = ?, f_var_type = ?, f_description = ? WHERE f_dag_id = ? AND f_var_name = ?`
				if err = store.db.Exec(sqlStr, v.DefaultValue, v.VarType, v.Description, dagID, v.VarName).Error; err != nil {
					return err
				}
			}
		}

		return nil
	}

	if !d.isTX {
		err = d.WithTransaction(newCtx, func(_ context.Context, txStore mod.Store) error {
			return fn(txStore.(*dag), dagVars, isCreate)
		})
	} else {
		err = fn(d, dagVars, isCreate)
	}

	return err
}
```

- [ ] **Step 3: Commit**

```bash
git add store/rds/dag/dag.go
git commit -m "feat: optimize CreateDagVars to avoid deadlock

- Add isCreate parameter to skip query/delete for new records
- Use diff-based approach for updates
- Only operate on changed rows

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: 优化 refreshDagIndexes 方法

**Files:**
- Modify: `store/rds/dag/dag.go:547-639`

- [ ] **Step 1: 添加 insertDagSteps 和 insertDagAccessors 辅助方法**

```go
// insertDagSteps 插入 step 索引
func (d *dag) insertDagSteps(ctx context.Context, steps []*DagStepModel) error {
	if len(steps) == 0 {
		return nil
	}

	sqlStr := `INSERT INTO t_flow_dag_step (f_id, f_dag_id, f_operator, f_source_id, f_has_datasource) VALUES `
	values := make([]any, 0, len(steps)*5)
	for _, row := range steps {
		sqlStr += "(?, ?, ?, ?, ?),"
		values = append(values, row.ID, row.DagID, row.Operator, row.SourceID, row.HasDatasource)
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")

	return d.db.Exec(sqlStr, values...).Error
}

// insertDagAccessors 插入 accessor 索引
func (d *dag) insertDagAccessors(ctx context.Context, accessors []*DagAccessorModel) error {
	if len(accessors) == 0 {
		return nil
	}

	sqlStr := `INSERT INTO t_flow_dag_accessor (f_id, f_dag_id, f_accessor_id) VALUES `
	values := make([]any, 0, len(accessors)*3)
	for _, row := range accessors {
		sqlStr += "(?, ?, ?),"
		values = append(values, row.ID, row.DagID, row.AccessorID)
	}
	sqlStr = strings.TrimSuffix(sqlStr, ",")

	return d.db.Exec(sqlStr, values...).Error
}
```

- [ ] **Step 2: 添加 refreshDagStepsWithDiff 和 refreshDagAccessorsWithDiff 方法**

```go
// refreshDagStepsWithDiff 使用 diff 方式刷新 step 索引
func (d *dag) refreshDagStepsWithDiff(ctx context.Context, dagID uint64, newSteps []*DagStepModel) error {
	existing, err := d.getExistingDagSteps(ctx, dagID)
	if err != nil {
		return err
	}

	diff := diffDagSteps(existing, newSteps)

	// 删除
	if len(diff.toDelete) > 0 {
		placeholders := make([]string, len(diff.toDelete))
		args := make([]any, len(diff.toDelete))
		for i, id := range diff.toDelete {
			placeholders[i] = "?"
			args[i] = id
		}
		sqlStr := fmt.Sprintf("DELETE FROM t_flow_dag_step WHERE f_id IN (%s)", strings.Join(placeholders, ","))
		if err = d.db.Exec(sqlStr, args...).Error; err != nil {
			return err
		}
	}

	// 插入
	if len(diff.toInsert) > 0 {
		if err = d.insertDagSteps(ctx, diff.toInsert); err != nil {
			return err
		}
	}

	// 更新
	if len(diff.toUpdate) > 0 {
		for _, s := range diff.toUpdate {
			sqlStr := `UPDATE t_flow_dag_step SET f_has_datasource = ? WHERE f_id = ?`
			if err = d.db.Exec(sqlStr, s.HasDatasource, s.ID).Error; err != nil {
				return err
			}
		}
	}

	return nil
}

// refreshDagAccessorsWithDiff 使用 diff 方式刷新 accessor 索引
func (d *dag) refreshDagAccessorsWithDiff(ctx context.Context, dagID uint64, newAccessors []*DagAccessorModel) error {
	existing, err := d.getExistingDagAccessors(ctx, dagID)
	if err != nil {
		return err
	}

	diff := diffDagAccessors(existing, newAccessors)

	// 删除
	if len(diff.toDelete) > 0 {
		placeholders := make([]string, len(diff.toDelete))
		args := make([]any, len(diff.toDelete))
		for i, id := range diff.toDelete {
			placeholders[i] = "?"
			args[i] = id
		}
		sqlStr := fmt.Sprintf("DELETE FROM t_flow_dag_accessor WHERE f_id IN (%s)", strings.Join(placeholders, ","))
		if err = d.db.Exec(sqlStr, args...).Error; err != nil {
			return err
		}
	}

	// 插入
	if len(diff.toInsert) > 0 {
		if err = d.insertDagAccessors(ctx, diff.toInsert); err != nil {
			return err
		}
	}

	return nil
}
```

- [ ] **Step 3: 重写 refreshDagIndexes 方法**

替换原有的 `refreshDagIndexes` 方法：

```go
func (d *dag) refreshDagIndexes(ctx context.Context, dag *entity.Dag, isCreate bool) error {
	var err error
	newCtx, span := trace.StartInternalSpan(ctx)
	defer func() { trace.TelemetrySpanEnd(span, err) }()

	if dag == nil {
		return nil
	}

	dagID, parseErr := strconv.ParseUint(dag.ID, 10, 64)
	if parseErr != nil {
		return parseErr
	}

	stepRows := BuildDagStepIndex(dag)
	accessorRows := BuildDagAccessorIndex(dag)

	// 处理 t_flow_dag_step 表
	if isCreate {
		if len(stepRows) > 0 {
			if err = d.insertDagSteps(newCtx, stepRows); err != nil {
				return err
			}
		}
	} else {
		if err = d.refreshDagStepsWithDiff(newCtx, dagID, stepRows); err != nil {
			return err
		}
	}

	// 处理 t_flow_dag_accessor 表
	if isCreate {
		if len(accessorRows) > 0 {
			if err = d.insertDagAccessors(newCtx, accessorRows); err != nil {
				return err
			}
		}
	} else {
		if err = d.refreshDagAccessorsWithDiff(newCtx, dagID, accessorRows); err != nil {
			return err
		}
	}

	return nil
}
```

- [ ] **Step 4: Commit**

```bash
git add store/rds/dag/dag.go
git commit -m "feat: optimize refreshDagIndexes to avoid deadlock

- Add isCreate parameter to skip query/delete for new records
- Use diff-based approach for updates

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 5: 更新调用方

**Files:**
- Modify: `store/rds/dag/dag.go`

- [ ] **Step 1: 更新 CreateDag 方法中的调用**

找到 `CreateDag` 方法中的调用（约第 388 行和 393 行），修改为：

```go
// 将：
err = store.CreateDagVars(newCtx, BuildDagVars(dagEntity))
// 改为：
err = store.CreateDagVars(newCtx, BuildDagVars(dagEntity), true)

// 将：
err = store.refreshDagIndexes(newCtx, dagEntity)
// 改为：
err = store.refreshDagIndexes(newCtx, dagEntity, true)
```

- [ ] **Step 2: 更新 UpdateDag 方法中的调用**

找到 `UpdateDag` 方法中的调用（约第 746 行和 751 行），修改为：

```go
// 将：
err = store.CreateDagVars(newCtx, BuildDagVars(dagEntity))
// 改为：
err = store.CreateDagVars(newCtx, BuildDagVars(dagEntity), false)

// 将：
err = store.refreshDagIndexes(newCtx, dagEntity)
// 改为：
err = store.refreshDagIndexes(newCtx, dagEntity, false)
```

- [ ] **Step 3: 运行测试验证**

Run: `cd /home/zhang/kweaver/kweaver-core/adp/dataflow/flow-automation && go build ./...`

Expected: 编译成功

- [ ] **Step 4: Commit**

```bash
git add store/rds/dag/dag.go
git commit -m "feat: update CreateDag and UpdateDag to use isCreate parameter

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 6: 添加并发测试

**Files:**
- Create: `store/rds/dag/concurrent_test.go`

- [ ] **Step 1: 编写并发测试**

```go
// store/rds/dag/concurrent_test.go
package dagmodel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kweaver-ai/kweaver-core/adp/dataflow/flow-automation/pkg/entity"
	"github.com/stretchr/testify/assert"
)

func TestConcurrentCreateDag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}

	d := NewDagRepository().(*dag)
	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make([]error, 10)
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			dag := &entity.Dag{
				ID:   fmt.Sprintf("test_dag_%d_%d", time.Now().UnixNano(), idx),
				Name: fmt.Sprintf("Concurrent Test DAG %d", idx),
				Vars: entity.DagVars{
					fmt.Sprintf("var_%d", idx): {DefaultValue: "val"},
				},
			}
			dag.Initial()

			_, err := d.CreateDag(ctx, dag)
			mu.Lock()
			errors[idx] = err
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Logf("Goroutine %d error: %v", i, err)
			assert.NotContains(t, err.Error(), "Deadlock", "Deadlock detected!")
		}
	}
}
```

- [ ] **Step 2: Commit**

```bash
git add store/rds/dag/concurrent_test.go
git commit -m "test: add concurrent test for deadlock verification

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 7: 验证和清理

- [ ] **Step 1: 运行所有测试**

Run: `cd /home/zhang/kweaver/kweaver-core/adp/dataflow/flow-automation && go test ./... -v -count=1`

Expected: PASS

- [ ] **Step 2: 运行代码检查**

Run: `cd /home/zhang/kweaver/kweaver-core/adp/dataflow/flow-automation && go vet ./... && go fmt ./...`

Expected: No errors

- [ ] **Step 3: 最终提交**

```bash
git add .
git commit -m "chore: final cleanup for deadlock fix

All tests pass. Code formatted and vetted.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## 验收清单

- [ ] diff 函数单元测试全部通过
- [ ] CreateDagVars 使用 diff 方式，支持 isCreate 参数
- [ ] refreshDagIndexes 使用 diff 方式，支持 isCreate 参数
- [ ] CreateDag 调用时传入 isCreate=true
- [ ] UpdateDag 调用时传入 isCreate=false
- [ ] 并发测试无死锁错误
- [ ] 所有现有测试通过
- [ ] 代码格式化通过 go fmt
- [ ] 代码检查通过 go vet
