# Perp Arb

perp 跨交易所套利, 在价差较大时，于 2 个交易所反向下单（一多一空），在价差收敛时 2 边同时平仓。

## 流程

0. 启动检查
  a. 启动时检查两个leg的初始仓位
  b. 如果存在 `mismatch >= config.min_order_size`，需要报警或退出
1. 轮询
   a. 同时请求 2 个 leg 的深度（挂单信息）, 缓存到 `best_orders`
   b. 计算每个 `direction` 的价差
2. 判断套利条件
   a. 遍历 `price_diff` 中每个 `direction` 的价差
   b. 如果某个 direction 的价差满足开仓或关仓条件，缓存该 `direction` 和 `action`
   c. 退出遍历
3. 根据 `direction` 和 `action` 执行订单
   a. `order_quantity = min(leg1 挂单数量， leg2 挂单数量，config.max_quantity )`
   b. `quantity < config.min_order_size` 跳过下单逻辑
   c. leg1 (binance) 以 maker 订单下单，等待结果
   d. websocket 接受订单结果
      - 当 leg1 limit order 完全成交， leg2 下市价单
      - 当 leg1 limit order 订单失败，结束本次执行
      - 当 leg1 limit order 订单超时未成交，取消订单，结束本次执行
4. mismatch： 2 个 leg 持仓数量绝对值的差值
   a.每隔一段时间检查 mismatch，判断仓位 mismatch 情况
   b. `mismatch_size < config.min_order_size` 忽略
   c. `mismatch_size >= config.max_quantity` 退出脚本，发送 tg 和 lark 报警消息

- 开仓条件:
  - `max(abs(leg1_position), abs(leg2_position)) < config.max_total_size - config.min_order_size`
  - `price_diff[direction] >= config.min_price_diff_open`
- 关仓条件:
  - `min(abs(leg1_position), abs(leg2_position)) >= config.min_order_size`
  - `price_diff[direction] >= config.min_price_diff_close`

## 相关定义

- `direction: str`: 开仓方向, "12" 代表 leg1 开多 leg2 开空，"21" 代表 leg1 开空 leg2 开多
- `best_orders`:

  ```python
  best_orders: {
      str: { # "12" | "21"
          "best_ask": [Decimal, Decimal], # [price, qty]
          "best_bid": [Decimal, Decimal], # [price, qty]
      }
  }
  ```

- `price_diff: {str: Decimal}`: 价差，分别缓存不同 direction 的价差
  - 总是用 (开空价格 - 开多价格)
  - 价格需要是相应方向能直接成交的 taker 价格
  - 例如 `direction = "12"`, `price_diff['12'] = leg1.best_ask - leg2.best_bid`
- `action: Literal["OPEN" | "CLOSE"]`: 开仓还是关仓，具体下单方向需要根据 `direction` 调整
  - 例如 `direction = "12"` OPEN leg1 开多 leg2 开空, CLOSE leg1 开空 leg2 开多

## 关键代码

### Config

```python
class PerpArbConfig:
    ticker: str
    contract_id_leg1: str
    contract_id_leg2: str
    exchange_leg1: str
    exchange_leg2: str
    max_quantity: Decimal # 单次套利下单的最大数量
    max_total_size: Decimal # 持仓总数量上限
    order_interval: int # 下单最小时间间隔 ms
    loop_interval: int # 轮询时间间隔 ms
    order_timeout: int # 订单超时时间 ms
    min_price_diff_open: Decimal # 套利开仓的最小价差
    min_price_diff_close: Decimal # 套利关仓的最小价差
    min_order_size: Decimal # 下单的最小数量
```
