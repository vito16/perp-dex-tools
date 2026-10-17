# 🐳 Docker Setup Guide

**For users who don't need to understand code.** Just follow these steps!

---

## 📋 Requirements

**Must Have:**
1. **Docker Desktop** - https://www.docker.com/products/docker-desktop
   - Make sure Docker is running (Docker icon in system tray)
   - **No Python needed!** Everything is in Docker.

2. **Exchange API keys** from your trading account

**Optional:**
- **Git** for easy updates (or download as ZIP from GitHub)

---

## 💻 How to Use Terminal

**First time?** Learn how to open terminal and find your project path.

### Open Terminal/Command Prompt
- **Mac**: Open "Terminal" app (press Command+Space, search "Terminal")
- **Windows**: Right-click in project folder → "Open in Terminal" or search "cmd" or "PowerShell"
- **Linux**: Right-click → "Open Terminal Here"

### Find Your Project Path

**If you used Git:**
```bash
# Remember where you downloaded it. Example:
cd ~/Downloads/perp-dex-tools
# Or:
cd /Users/yourname/Projects/perp-dex-tools
```

**If you downloaded ZIP:**
1. Unzip the file to an easy location (like Desktop or Documents)
2. Remember this location
3. In terminal, type `cd` (with space after), then **drag** the folder into terminal window
4. Press Enter

**Check if you're in the right place:**
```bash
# Mac/Linux:
ls
# Windows:
dir

# You should see: docker-compose.yml, run.sh, trading_bot.py, etc.
```

---

## 🚀 Quick Start

### 1. Get the Project
```bash
# With Git:
git clone https://github.com/LanQikun/perp-dex-tools
cd perp-dex-tools

# Or download ZIP from GitHub and unzip
```

### 2. Configure API Keys
```bash
# Copy template
cp env_example.txt .env

# Edit .env with your text editor and add your API keys
```

### 3. Configure Trading (edit `docker-compose.yml`)
```yaml
command: [
  "--exchange", "edgex",      # Exchange: edgex, backpack, aster, etc.
  "--ticker", "ETH",          # Coin: ETH, BTC, SOL, etc.
  "--quantity", "0.1",        # Order size
  "--take-profit", "0.02",    # Profit in USDT
  "--direction", "buy"        # "buy" or "sell"
]
```

### 4. Start Trading

**Run in terminal:** (New to terminal? See "How to Use Terminal" section above)
```bash
```bash
# Mac/Linux:
./run.sh

# Windows:
run.bat

# Or manually:
docker-compose up --build
```

**Stop**: Press `Ctrl+C` or run `./stop.sh` (Mac/Linux) / `stop.bat` (Windows)

**⚠️ If build fails (especially in China):**
Network issues may cause failures. **Just try again!**
```bash
# First, just retry (fastest):
./run.sh

# If fails 2-3 times, clean up and retry:
docker-compose down
docker system prune -f
./run.sh
```

💡 **Tip**: Slow internet? First failure → just retry. Multiple failures → clean up first. Docker caches successful steps, so each retry gets further.

---

## 🔄 Changed Configuration?

**Good news: Configuration changes don't require rebuilding the image!**

### Just Restart (Fast)
If you changed:
- Trading parameters in `docker-compose.yml` (exchange, ticker, quantity, etc.)
- API keys in `.env` file

```bash
# Method 1: Restart container (fastest, 5-10 seconds)
docker-compose restart

# Method 2: Stop and start
docker-compose down
docker-compose up
```

### Need to Rebuild (Slow)
**Only** rebuild if you changed:
- `Dockerfile`
- `requirements.txt` (added/removed Python packages)
- Python code files (`trading_bot.py`, `exchanges/`, etc.)

```bash
# Rebuild and start (4-8 minutes)
docker-compose up --build
```

💡 **Why?** Configuration parameters are read when the container starts, not baked into the image. Only code and dependency changes require rebuilding the image.

---

## 🇨🇳 For Users in China

### Configure Docker Mirrors (One-Time Setup):

**1. Docker Desktop → Settings → Docker Engine**, add:
```json
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com"
  ]
}
```
Click "Apply & Restart"

**2. The Dockerfile is already configured** with:
- ✅ apt mirrors (system packages)
- ✅ pip mirrors (Python packages)
- ✅ Build time: 4-8 minutes (vs 20-60 without mirrors)

**3. If git dependencies fail:**
- Option A: Configure proxy in Docker Desktop (Settings → Resources → Proxies)
- Option B: Use VPN
- Option C: Try direct connection (sometimes works)

---

## 📊 View Logs

Logs are saved in `logs/` folder on your computer. Open with Excel or any text editor.

**View live logs:**
```bash
docker-compose logs -f trading-bot
```

---

## 🔄 Updates

```bash
# With Git:
git pull

# If only config changed (docker-compose.yml, .env):
docker-compose restart

# If code or dependencies changed:
docker-compose up --build

# Without Git:
# Download new ZIP, copy your .env to new folder, run again
```

---

## 🆕 After Code Update?

**After the project code is updated, follow these steps:**

### Step 1: Pull Latest Code
```bash
# With Git:
cd /path/to/perp-dex-tools
git pull

# Or download latest ZIP and extract
```

### Step 2: Check What Changed
Look for changes to:
- **requirements.txt** → Need to rebuild
- **Python code** (.py files) → Need to rebuild
- **Dockerfile** → Need to rebuild
- **docker-compose.yml** → Just restart
- **env_example.txt** new variables → Update your .env file

### Step 3: Rebuild and Start
```bash
# Stop existing container
docker-compose down

# Rebuild (ensures using latest code)
docker-compose up --build
```

### Step 4: Check Logs
```bash
# Make sure bot is running correctly
docker-compose logs -f trading-bot
```

💡 **Tips**:
- Backup your `.env` file before updating (`cp .env .env.backup`)
- If new version added environment variables, compare `env_example.txt` and update your `.env`
- If you encounter issues after update, try cleaning: `docker-compose down && docker system prune -f`

---

## 📝 Trading Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--exchange` | Exchange to use | `edgex`, `backpack`, `aster` |
| `--ticker` | Trading pair | `ETH`, `BTC`, `SOL` |
| `--quantity` | Order size | `0.1` |
| `--take-profit` | Profit target (USDT) | `0.02` |
| `--direction` | Buy or sell | `buy` or `sell` |
| `--max-orders` | Max active orders | `40` |
| `--wait-time` | Seconds between orders | `450` |
| `--boost` | Boost mode (Aster/Backpack) | Add flag to enable |

---

## 🆘 Troubleshooting

### "Docker is not running"
- Start Docker Desktop

### "Env file not find"
- Make sure you created `.env` from `env_example.txt`
- Check the filename is exactly `.env` (not `.env.txt`)

### "Cannot connect to exchange"
- Check API keys in `.env`
- Verify API keys have trading permissions
- Check internet connection

### Build is slow (China users)
- Configure Docker mirrors (see China section above)

### Build fails with "connection timeout" or "network error"
**Common in China due to slow/unstable connections. Solution: Retry!**
```bash
# Step 1: Simple retry (recommended first)
./run.sh

# Step 2: If still fails after 2-3 tries, clean up:
docker-compose down
docker system prune -f
./run.sh

# Step 3: Last resort (removes all cache):
docker-compose build --no-cache
docker-compose up
```
💡 Try Step 1 first - it's fastest! Only use Step 2 if you get repeated failures. Step 3 is rarely needed.

### "Permission denied" (Mac/Linux)
```bash
chmod +x run.sh stop.sh
```

### Start fresh
```bash
docker-compose down
docker system prune -f
docker-compose up --build
```

---

## 🎯 Multiple Bots

Edit `docker-compose.yml` and uncomment example configurations:

```yaml
trading-bot-backpack:
  extends: trading-bot
  container_name: trading-bot-backpack
  command: ["--exchange", "backpack", "--ticker", "SOL", ...]
```

Start specific bot:
```bash
docker-compose up trading-bot-backpack -d
```

---

## ⏱️ Expected Times

**First run:**
- Building image: 4-8 minutes (with mirrors) or 20-60 minutes (without)
- Starting bot: 5-10 seconds

**Subsequent runs:**
- Starting bot: 5-15 seconds (cached image)

**Updates:**
- Rebuild: 1-3 minutes (cached layers)

---

## 🔒 Security

- ✅ Never share your `.env` file
- ✅ `.env` is in `.gitignore` (won't be committed)
- ✅ Use API keys with only trading permissions (no withdrawal)
- ✅ Test with small amounts first

---

**Need help?** Check the main [README.md](README.md) for more details.

**Happy Trading! 📈**
