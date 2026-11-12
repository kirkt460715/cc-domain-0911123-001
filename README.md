# 登记交换台账

接收登记机构的有序事件，维护资产历史、冲突处置及按提交位置消费的增量视图。接收位置与提交位置分别记录。

Python 3.11 标准库与 SQLite 即可运行。数据、时钟和文件均在本地，不需要服务凭据。示例只使用合成记录。

## 运行

在仓库根目录执行：

```sh
python -m registry_bridge --db :memory: demo --fixture fixtures/demo.json
python -m examples.context
python -m unittest discover -s tests -v
python -m compileall -q registry_bridge examples
```

示例使用内存或临时目录，不会改动持久台账。`examples/context.json` 记录一组可重复导入的业务输入，字段与时间口径见 `examples/数据说明.md`。`examples.context` 检查数据并调用现有产品入口；它只展示原有业务数据，不计算分析结论。

`tests` 覆盖现有业务生命周期、非法输入、版本和完整性边界，另有示例导入回归。完整测试和上述演示可在一次短终端会话中执行。

## 登记量“当时能知道多少”分析

`registry_bridge.analysis` 回答运营场景中的核心问题：周一看到的新增量下降，有多少是真实减少，有多少只是登记还没进入可查询台账。分析按**登记生效周**形成队列，统计单位是机构内**首次成功 register** 的资产；amend、重复投递和后续状态变化都不再算新增。可知时点统一取 `applied_at`，收到但尚未投影的登记在 `received_not_projected` 中单列。

分析参数（业务周范围、知识截止 `knowledge_cutoff`、IANA 时区、最大成熟滞后 `max_maturity_lag_weeks`、最小历史周数、bootstrap 分位数与种子）显式传入。负延迟（`applied_at` 早于 `effective_at`）整周移出训练样本并在 `anomalies.negative_lag` 报告；成熟期之后仍有登记到达时，`anomalies.residual_underreporting_possible` 置真，提示漏报并未被证明已消除。

- 只用知识截止前**已走完成熟期**的历史队列估计累计到达比例（按历史周整体重采样的 bootstrap 区间）。
- 机构分别建模。小机构可经 `allow_borrow=true` 与 `borrow_groups` 显式借用同组机构历史，报告 `profile.donor_issuers` 与 `borrowed`；历史不足、到达比例为零或明显不稳定时保留 `known_count` 观测量并拒绝给确定估计（状态为 `insufficient_history` / `ratio_zero` / `unstable`）。
- 分析结果内容寻址保存（`analysis_id` 为去除时间戳后文档的摘要）。补报后用 `arrival.revise` 生成新版本并按生效周列出新增可知登记；已发版本不会被覆盖，仍可凭原 id 或导出文件原样取回。
- `arrival.hindcast` 做历史滚动回看：每个回看点只用那时可知的记录和成熟周训练，并与“已知量即最终量”的朴素基线比较 MAE，同时用置换检验标记迟报机制改变的机构。

```sh
python -m registry_bridge --db registry.sqlite3 arrivals analyze --input analysis-params.json
python -m registry_bridge --db registry.sqlite3 arrivals hindcast --input analysis-params.json
python -m registry_bridge --db registry.sqlite3 arrivals revise  --input revise-params.json
python -m registry_bridge --db registry.sqlite3 arrivals list
python -m registry_bridge --db registry.sqlite3 arrivals get --id <analysis_id>
python -m registry_bridge --db registry.sqlite3 arrivals export --id <analysis_id> --path out.json
python -m registry_bridge --db registry.sqlite3 arrivals import --path out.json
```

对应服务操作为 `arrival.analyze`、`arrival.hindcast`、`arrival.revise`、`arrival.get`、`arrival.list`、`arrival.export`、`arrival.import`。`tests/analysis/test_arrivals.py` 覆盖队列口径、未投影单列、负延迟剔除、借用选择、拒绝估计、种子可复现、内容寻址持久化、补报修订、滚动回看与按接收时间交错投递的端到端演练。

## 容器

```sh
docker build -t local/registry_bridge .
docker run --rm --network none local/registry_bridge
docker run --rm --network none local/registry_bridge python -m examples.context
```

镜像构建完成后运行和测试不下载文件。持久化使用各自命令的本地路径参数；具体操作及参数可通过 `python -m registry_bridge --help` 查询。第三方依赖清单为空。
