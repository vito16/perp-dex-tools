# 🐳 Docker 使用指南

**给不懂代码的用户。** 只需按照以下步骤操作！

---

## 📋 准备工作

**必需：**
1. **Docker Desktop** - https://www.docker.com/products/docker-desktop
   - 确保 Docker 正在运行（系统托盘中有 Docker 图标）
   - **不需要安装 Python！** 所有内容都在 Docker 中。

2. **交易所 API 密钥**（从您的交易账户获取）

**可选：**
- **Git** 方便更新（或从 GitHub 下载 ZIP）

---

## 💻 如何使用终端

**第一次使用？** 先学会打开终端和找到项目路径。

### 打开终端/命令行
- **Mac**：打开"终端"应用（按 Command+空格 搜索 "Terminal"）
- **Windows**：在项目文件夹中右键 → "在终端中打开" 或搜索 "cmd" 或 "PowerShell"
- **Linux**：右键 → "在此处打开终端"

### 找到项目路径

**如果用 Git 下载：**
```bash
# 记住你下载到哪里了。例如：
cd ~/Downloads/perp-dex-tools
# 或
cd /Users/你的用户名/Projects/perp-dex-tools
```

**如果下载 ZIP：**
1. 解压 ZIP 文件到一个容易找的地方
2. 记住这个位置
3. 在终端中输入 `cd`（后面加空格），然后把文件夹**拖**到终端窗口
4. 按回车

**检查是否在正确位置：**
```bash
# Mac/Linux：
ls
# Windows：
dir

# 应该能看到：docker-compose.yml, run.sh, trading_bot.py 等文件
```

---

## 🚀 快速开始

### 1. 获取项目
```bash
# 使用 Git：
git clone https://github.com/LanQikun/perp-dex-tools
cd perp-dex-tools

# 或从 GitHub 下载 ZIP 并解压
```

### 2. 配置 API 密钥
```bash
# 复制模板
cp env_example.txt .env

# 用文本编辑器（建议使用visual studio code）打开 .env 并添加您的 API 密钥
```

### 3. 配置交易参数（编辑 `docker-compose.yml`）
```yaml
command: [
  "--exchange", "edgex",      # 交易所：edgex, backpack, aster 等
  "--ticker", "ETH",          # 币种：ETH, BTC, SOL 等
  "--quantity", "0.1",        # 订单大小
  "--take-profit", "0.02",    # 利润目标（USDT）
  "--direction", "buy"        # "buy" 或 "sell"
]
```

### 4. 开始交易

**在终端中运行：**（不会用终端？看上面"如何使用终端"部分）
```bash
# Mac/Linux：
./run.sh

# Windows：
run.bat

# 或手动：
docker-compose up --build
```

**停止**：按 `Ctrl+C` 或运行 `./stop.sh`（Mac/Linux）/ `stop.bat`（Windows）

**⚠️ 如果构建失败（特别是在中国）：**
网络问题可能导致失败。**直接重试即可！**
```bash
# 首先，直接重试（最快）：
./run.sh

# 如果失败 2-3 次，清理后重试：
docker-compose down
docker system prune -f
./run.sh
```

💡 **提示**：网速慢？第一次失败 → 直接重试。多次失败 → 先清理再试。Docker 会缓存成功的步骤，每次重试都会继续前进。

---

## 🔄 修改配置后怎么办？

**好消息：修改配置不需要重新构建镜像！**

### 只需重启（快速）
如果你修改了：
- `docker-compose.yml` 中的交易参数（交易所、币种、数量等）
- `.env` 文件中的 API 密钥

```bash
# 方法 1：重启容器（最快，5-10秒）
docker-compose restart

# 方法 2：停止后重新启动
docker-compose down
docker-compose up
```

### 需要重新构建（慢）
**只有**修改了这些才需要重新构建：
- `Dockerfile`
- `requirements.txt`（添加/删除 Python 包）
- Python 代码文件（`trading_bot.py`、`exchanges/` 等）

```bash
# 重新构建并启动（4-8分钟）
docker-compose up --build
```

💡 **为什么？** 配置参数是容器启动时读取的，不影响镜像内容。只有代码和依赖变化才需要重新构建镜像。

---

## 🇨🇳 中国用户配置

### 配置 Docker 镜像源（一次性设置）：

**1. Docker Desktop → Settings → Docker Engine**，添加：
```json
{
  "registry-mirrors": [
    "https://docker.1panel.live",
    "https://docker.1ms.run",
    "https://docker.xpg666.xyz",
    "https://docker-0.unsee.tech",
    "https://docker.m.daocloud.io"
  ]
}
```
例子（我电脑上的，你的或许不同。注意：在"experimental": false后添加一个逗号）：
```json
{
  "builder": {
    "gc": {
      "defaultKeepStorage": "20GB",
      "enabled": true
    }
  },
  "experimental": false,
  "registry-mirrors": [
    "https://docker.1panel.live",
    "https://docker.1ms.run",
    "https://docker.xpg666.xyz",
    "https://docker-0.unsee.tech",
    "https://docker.m.daocloud.io"
  ]
}
```
点击 "Apply & Restart"

**2. 如果 git 依赖下载失败：**
- 方案 A：在 Docker Desktop 配置代理（Settings → Resources → Proxies）
- 方案 B：使用 VPN
- 方案 C：尝试直连（有时可以）

---

## 📊 查看日志

日志保存在您电脑的 `logs/` 文件夹中。可用 Excel 或任何文本编辑器打开。

**查看实时日志：**
```bash
docker-compose logs -f trading-bot
```

---

## 🔄 更新

```bash
# 使用 Git：
git pull

# 如果只是配置改动（docker-compose.yml, .env）：
docker-compose restart

# 如果代码或依赖改动：
docker-compose up --build

# 不使用 Git：
# 下载新的 ZIP，将您的 .env 复制到新文件夹，重新运行
```

---

## 🆕 代码更新后怎么办？

**项目代码更新后，按以下步骤操作：**

### 步骤 1：拉取最新代码
```bash
# 使用 Git：
cd /path/to/perp-dex-tools
git pull

# 或下载最新 ZIP 并解压
```

### 步骤 2：检查更新内容
查看是否有以下变化：
- **requirements.txt** 改动 → 需要重新构建
- **Python 代码**（.py 文件）改动 → 需要重新构建
- **Dockerfile** 改动 → 需要重新构建
- **docker-compose.yml** 改动 → 只需重启
- **.env.example** 新增变量 → 需要更新你的 .env 文件

### 步骤 3：重新构建并启动
```bash
# 停止现有容器
docker-compose down

# 重新构建（确保使用最新代码）
docker-compose up --build
```

### 步骤 4：检查日志
```bash
# 确保机器人正常运行
docker-compose logs -f trading-bot
```

💡 **提示**：
- 更新前先备份你的 `.env` 文件（`cp .env .env.backup`）
- 如果新版本增加了新的环境变量，对比 `env_example.txt` 更新你的 `.env`
- 更新后如果遇到问题，可以先清理：`docker-compose down && docker system prune -f`

---

## 📝 交易参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| `--exchange` | 使用的交易所 | `edgex`, `backpack`, `aster` |
| `--ticker` | 交易对 | `ETH`, `BTC`, `SOL` |
| `--quantity` | 订单大小 | `0.1` |
| `--take-profit` | 利润目标（USDT） | `0.02` |
| `--direction` | 买入或卖出 | `buy` 或 `sell` |
| `--max-orders` | 最大活跃订单数 | `40` |
| `--wait-time` | 订单间隔秒数 | `450` |
| `--boost` | 提升模式（Aster/Backpack） | 添加标志启用 |

---

## 🆘 故障排除

### "Docker is not running"（Docker 未运行）
- 启动 Docker Desktop

### "Env file not find"（找不到环境文件）
- 确保您从 `env_example.txt` 创建了 `.env`
- 检查文件名是否完全为 `.env`（不是 `.env.txt`）

### "Cannot connect to exchange"（无法连接交易所）
- 检查 `.env` 中的 API 密钥
- 验证 API 密钥有交易权限
- 检查网络连接

### 构建速度慢（中国用户）
- 配置 Docker 镜像（见上方中国用户配置部分）

### 构建失败，显示 "connection timeout" 或 "network error"
**在中国很常见，网络慢/不稳定。解决方法：重试！**
```bash
# 步骤 1：简单重试（推荐优先使用）
./run.sh

# 步骤 2：如果重试 2-3 次还失败，清理后重试：
docker-compose down
docker system prune -f
./run.sh

# 步骤 3：最后手段（删除所有缓存）：
docker-compose build --no-cache
docker-compose up
```
💡 先试步骤 1 - 最快！只有反复失败才用步骤 2。步骤 3 很少需要。

### "Permission denied"（权限被拒绝）- Mac/Linux
```bash
chmod +x run.sh stop.sh
```

### 重新开始
```bash
docker-compose down
docker system prune -f
docker-compose up --build
```

---

## 🎯 运行多个机器人

编辑 `docker-compose.yml` 并取消注释示例配置：

```yaml
trading-bot-backpack:
  extends: trading-bot
  container_name: trading-bot-backpack
  command: ["--exchange", "backpack", "--ticker", "SOL", ...]
```

启动特定机器人：
```bash
docker-compose up trading-bot-backpack -d
```

---

## ⏱️ 预期时间

**首次运行：**
- 构建镜像：4-8 分钟（配置镜像）
- 启动机器人：5-10 秒

**后续运行：**
- 启动机器人：5-15 秒（使用缓存）

**更新：**
- 重新构建：1-3 分钟（使用缓存层）

---

## 🔒 安全提示

- ✅ 永远不要分享您的 `.env` 文件
- ✅ `.env` 已在 `.gitignore` 中（不会被提交）
- ✅ 使用仅有交易权限的 API 密钥（不要有提现权限）
- ✅ 先用小额测试

---

**需要帮助？** 查看主 [README.md](README.md) 了解更多详情。

**祝交易愉快！📈**
