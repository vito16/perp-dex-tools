# Backpack-Paradex 对冲模式使用指南

本模式专门用于在 **Backpack Exchange** (作为 Maker 挂单) 和 **Paradex Exchange** (作为 Taker 立即对冲) 之间进行永续合约对冲交易。

## 核心策略逻辑
1.  **Maker 挂单 (Backpack)**：程序在 Backpack 上以当前 BBO（最佳买价/卖价）挂出一个 Post-Only 订单。
2.  **等待成交**：程序监听 Backpack 的 WebSocket，一旦挂单成交，立即触发对冲逻辑。
3.  **Taker 对冲 (Paradex)**：在 Paradex 上以市价单（Market Order）模式立即吃单，确保仓位实时对冲。
4.  **循环执行**：
    - 阶段一：在 Backpack 买入，在 Paradex 卖出（累积仓位直到达到 `max_position`）。
    - 阶段二：在 Backpack 卖出，在 Paradex 买入（平掉仓位）。

## 环境配置
请确保 `.env` 文件包含以下 Paradex 相关的配置：
```env
PARADEX_L1_ADDRESS=您的L1钱包地址
PARADEX_L2_PRIVATE_KEY=您的L2私钥
PARADEX_ENVIRONMENT=prod  # 或 testnet
```

## 使用命令
使用 `hedge_mode.py` 脚本并指定 `--exchange backpack_paradex`：

```bash
python3 hedge_mode.py --exchange backpack_paradex --ticker <TICKER> --size <单笔币数> --iter <循环次数> --sleep <循环间休眠秒数>
```

### 示例
以 SOL 为例，每笔下单 0.3 SOL，运行 2 个循环：
```bash
python3 hedge_mode.py --exchange backpack_paradex --ticker SOL --size 0.3 --iter 2 --sleep 90
```

## 统计与日志说明
- **控制台输出**：程序会实时显示 Backpack 的挂单状态（⏳ 下单中、✅ 已挂单、💰 已成交）以及 Paradex 的对冲状态。
- **结算单 (Cycle Summary)**：每一轮对冲（一买一卖）完成后，会打印本轮的详细结算单，包括：
    - 两边的真实成交价。
    - 两边产生的真实手续费。
    - 最终的净损益 (Net PnL)。
- **日志文件**：
    - 详细日志：`logs/backpack_paradex_<TICKER>_hedge_mode_log.txt`
    - 成交记录 (CSV)：`logs/backpack_paradex_<TICKER>_hedge_mode_trades.csv`
    - 分交易所活动记录：`logs/backpack_<TICKER>_activity.log` 和 `logs/paradex_<TICKER>_activity.log`
