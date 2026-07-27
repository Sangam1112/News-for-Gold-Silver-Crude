# News for Gold | Silver | Crude 📈🛢🥇🥈

A dedicated Python script and automated tracker for US Economic Calendar events that impact prices of **Gold**, **Silver**, and **Crude Oil**.

All event release schedules and alerts are converted and reported in **Indian Standard Time (IST)**.

---

## ✨ Features

- **Real-Time Data**: Queries live economic calendar APIs (e.g. Nasdaq Economic Calendar).
- **Dual Alerts**:
  - ⏰ **2 Hours Before Event (IST)**: Gives heads-up notice, consensus vs previous expectations, and commodity impact guidance.
  - 🚨 **At Event Release Time (IST)**: Sends actual released numbers vs forecasts.
- **Daily Digest**: Morning schedule summary of all major US economic releases for the day in IST.
- **Telegram Notifications**: Dispatches alerts directly to Telegram using bot credentials loaded from `.env`.
- **Smart Grouping**: Groups simultaneous reports (e.g., EIA Crude Inventories + Gasoline + Cushing) into a single clean message.
- **State Preservation**: Avoids sending duplicate alerts upon restart or cron polling.

---

## 📊 Tracked Commodity Events

| Category | Events Tracked | Asset Impact |
|---|---|---|
| 🛢 **Crude Oil** | EIA Weekly Crude Inventories, Cushing Stocks, Gasoline Stocks, Distillate Stocks, OPEC Meetings, API Weekly Crude | Crude Oil |
| 🥇 **Gold & Silver** | FOMC Interest Rate Decision, FOMC Statement, CPI, Core CPI, PPI, PCE Price Index, Non-Farm Payrolls (NFP), Unemployment Rate, GDP, ISM Manufacturing/Services PMI, Jobless Claims | Gold, Silver |

---

## 🚀 Getting Started

### 1. Prerequisites & Installation

```bash
git clone https://github.com/Sangam1112/News-for-Gold-Silver-Crude.git
cd News-for-Gold-Silver-Crude
pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file in your home directory (`/home/sankita/.env`) or local directory:

```env
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

---

## 💻 Usage Commands

### View Today's Schedule in Terminal (IST)
```bash
python3 us_economic_calendar_tracker.py --today
```

### Test Telegram Notification
```bash
python3 us_economic_calendar_tracker.py --test
```

### Run Continuous Daemon Mode (24/7 Monitoring)
```bash
python3 us_economic_calendar_tracker.py --daemon
```

### Run Single Check via Cron
```bash
python3 us_economic_calendar_tracker.py --check
```

Example Crontab entry (every 5 minutes):
```cron
*/5 * * * * /usr/bin/python3 /home/sankita/News-for-Gold-Silver-Crude/us_economic_calendar_tracker.py --check >> /tmp/calendar_tracker.log 2>&1
```

---

## 📜 License

MIT License
