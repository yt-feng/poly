# BTC 五分钟事件：微观结构研究与数据采集 v3

## 目标与边界

本轮把新增模块合并进现有 `capture-v2` 工作流，入口改为 `capture_v3.py`；继承 v2 的采样、连接重试、分块校验与 Release 发布。原 `capture_v2.py`、旧 CSV、旧采集任务均保留。新入口默认 BTC，仅做公开数据采集，不连接钱包、不下单、不绕过访问或地区限制。

关注的问题是：**合约实际依据什么结算；可见价格能否以指定数量成交；外部订单流是否领先；成本和执行延迟是否吞噬价差；最后如何用无未来信息的数据检验。** 采集完整度不能等同于交易策略有效性。

## 优先框架与已实现数据

| 层次 | 数据与频率 | 研究用途与注意事项 |
|---|---|---|
| 1 合约与结算依据 | 当前/下一 BTC 5m Gamma 元信息约30秒刷新；前一小时12个窗口约120秒复核；规则哈希、condition/token、开闭市、最小订单、tick、费用参数、奖励门槛；官方发布的 priceToBeat 及读取时间 | 不能用第一笔成交、取整 Binance 开盘价或后知价格替代合约起始价。结算价格源逐市场识别，缺失时明确 unknown。 |
| 2 正确价格源 | Chainlink 30秒/60秒 TWAP 实时主题与原 v2 spot 价格分别保留；E18值用 Decimal 无损保存 | 已留存的2026-09-22 BTC5m市场样本明确使用60秒TWAP。不要假定所有未来市场永远相同，也不把spot当TWAP。TWAP是回看窗口而非推送频率；断线没有RTDS历史回放。 |
| 3 可成交价格与规模 | 复用两侧REST原始盘口；每秒算L1/5/10/20深度、价差、中间价、microprice、imbalance；10/100/500/1000股逐档扫描买卖成本 | 双腿须新鲜≤1500ms、读取时间差≤250ms、双边有效、市场可交易、最小订单已知且满足、足额深度；费用未知则不输出扣费后结果。 |
| 4 订单流 | 复用Binance spot逐笔；新增永续aggTrade与Coinbase matches；1/5/15/60秒买卖名义量、差值、主动方向与连接内累计差额 | 基于本地收到的消息，保留交易ID间断/暖机/断线标记；Coinbase side为maker方向，需要反转。公共盘口数量下降不被直接称为撤单。 |
| 5 多交易场所验证 | 新增Binance spot20档100ms；Coinbase BTC-USD ticker、公开level2_batch与matches；USDT-USD ticker | USD/USDT不默认为1:1；汇率不新鲜则USD跨市场basis为空。Coinbase量为绝对值、0删除，同价文本规范化，重连重建。跨市场basis仅指示性，不是同步可成交价差。 |
| 6 永续压力 | 正确路由的Binance USD-M市场/公共WS：aggTrade、mark/index/funding/nextFundingTime、bookTicker、forceOrder；OI30秒；OI历史/主动买卖比/历史funding约5分钟 | 合约-现货基差、杠杆拥挤/去杠杆背景。强平流是交易所抽样通知，不是全量强平。慢频指标不能伪装成逐秒变化。受限接口保持不可用，不切换地区或冒充现货。 |
| 7 相邻市场 | 当前BTC15m元信息与双侧盘口约15秒 | 仅作概率/情绪对照；窗口和起始价不同，不能拼成无风险套利。 |
| 8 时间与可观察性 | 每条原始消息保存本地wall-clock纳秒及monotonic纳秒；公开HTTP计时；60秒探测服务端时间；每秒有效性、age/skew；5/15/60秒因果收益与实现波动 | source event time、publisher time、received time分开。HTTP RTT/中点时差只是诊断，含时钟精度与路径不对称误差，不能当单向延迟或交易领先证据。 |
| 9 事后标签 | 单独labels分块：1/5/30秒quote markout；CLOB明确winner与WSmarket_resolved；记录label available time | 不回写旧特征。quote markout不是实际成交PnL；窗口切换/旧价/缺口会标无效；末尾未到期标签保持pending。CLOB winner读取时间不冒充实际结算时间。 |

## 费用与价差估计

当前文档曲线为 `C * rate * p * (1-p)`；代码只在该市场明确给出费用开启且 exponent=1 时采用市场 rate，或在明确关闭费用时采用0。旧 `fee-rate` bps 接口响应另存作核对，不直接解释成曲线 rate。未知、变更或不支持参数不会被静默填0。

`complete_set_quotes` 对不同数量逐档估计两腿成本及 USDC 等值手续费，另给买齐一套/已有库存卖出一套的纸面差额。**不代表可执行套利或净利润**：真实成交净股数、手续费扣收币种、逐笔舍入、共享/镜像流动性、两腿非原子执行、排队、滑点、资金占用和gas都要在执行层核实。没有私有成交记录时，不声称已实现收益；maker rebates和奖励不预支为确定收入。

## 接入与数据位置

- 生产仍为 `.github/workflows/capture-v2.yml`，同一生产 concurrency group、4小时轮换与每小时排队，无并行重复生产采集器。已启动的旧版本运行不会热更新；合并后的队列/下一次运行使用新入口。
- `capture-v2-<run>-<attempt>` Releases：现有 `raw-*.jsonl.gz` 保留新增源；`snapshots-*.jsonl.gz` 保持顶层schema2兼容，并加入 `microstructure.schema_version=3`；`labels-*` 和 `market_changes-*` 单独分块，全部继承manifest与SHA-256校验。
- `quality.json/.md` 新增每个源的有效样本数；缺失/受限永续不伪装成有数据。对齐后再测信号，不能用“workflow绿色”代替覆盖审计。
- 历史回补同一工作流顺序执行原spot任务和BTCUSDT USD-M任务；后者每轮最多12文件、512MiB、24次尝试。两者共享持久化去重状态、按market区分key，并输出独立 `backfill-summary-spot.json`、`backfill-summary-futures-um.json`。历史trades/aggTrades/1m K线不重建未记录的盘口/OI/强平/TWAP。
- 当前/近一小时窗口结算复核是有限范围，不宣称跨停机多小时的结算标签全集；RTDS TWAP历史和旧微观盘口也没有被追溯补齐。

运行与检查：

```bash
pip install -r requirements-v2.txt
python -m unittest discover -s tests -p 'test_*.py' -v
python capture_v3.py --assets btc --seconds 120 --output smoke_v3 --require-core --require-microstructure
```

严格短测要求至少取得5条有效的规则、费用、匹配的结算参考价、spot20档及Coinbase数据；永续与汇率独立显示可用性，避免因额外源受限停止既有核心记录。合并前需查看真实短测产物，不以本地单测替代网络验证。

## 仍需私有执行系统的数据（本轮没有获取）

最有价值的下一层是自己的订单发送/服务端确认/部分成交/撤单确认时间、order/trade ID、净股数、扣费明细、订单前可见排队量、库存、实际滑点与成交后markout。公共L2不能确定真实队列位置或成交概率；只有授权的私有账户流才能核实。此仓库公开，任何私有交易日志或密钥都不应写入其Git历史、公开Release或CI日志。本轮不访问这些接口。

不默认扩展全币种、社交舆情、全链地址画像与完整期权链：它们增加数据规模，但不能先替代正确结算依据、费用和逐笔执行验证。

## 协议依据（本轮核对日2026-09-23；上线以实测可用性为准）

- Polymarket TWAP公开主题、E18、无历史回放：https://docs.polymarket.com/market-data/chainlink-twap
- 市场费用曲线及舍入：https://docs.polymarket.com/trading/fees
- 每市场参数：https://docs.polymarket.com/market-data/market-details
- Polymarket公开WS与生命周期：https://docs.polymarket.com/market-data/realtime-data
- Coinbase公开channels与maker side：https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
- Binance USD-M最新路由及变更：https://developers.binance.com/docs/derivatives/change-log
- Binance公开spot streams：https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
- Binance原始历史归档：https://github.com/binance/binance-public-data
