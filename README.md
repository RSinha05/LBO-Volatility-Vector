# LBO Intelligence Platform

Global LBO screener covering **NASDAQ · DOW JONES · NSE NIFTY 50 · BSE SENSEX**.  
FastAPI backend + single-page frontend, served from one container.

---

## Deploy with Docker + GitHub (step-by-step)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed on your machine  
  (or Docker Engine on a Linux server)
---

### Step 1 — Push to GitHub

**Option A — GitHub web UI (no Git needed)**

1. Go to [github.com/new](https://github.com/new)
2. Repository name: `lbo-platform`
3. Visibility: Public or Private (your choice)
4. Click **Create repository**
5. On the next page click **uploading an existing file**
6. Drag and drop **all files** from this folder (including the `frontend/` subfolder)
7. Click **Commit changes**

**Option B — Git CLI**

```bash
cd lbo_platform          # this folder

git init
git add .
git commit -m "initial deploy"

# Replace YOUR_USERNAME with your GitHub username
git remote add origin https://github.com/YOUR_USERNAME/lbo-platform.git
git branch -M main
git push -u origin main
```

---

### Step 2 — Run locally with Docker

```bash
# Clone your repo (or just stay in this folder)
git clone https://github.com/YOUR_USERNAME/lbo-platform.git
cd lbo-platform

# Build and start (one command)
docker compose up --build

# App is live at:
#   http://localhost:8000          ← frontend + app
#   http://localhost:8000/docs     ← interactive API docs
#   http://localhost:8000/api/health
```

To stop:
```bash
docker compose down
```

To run in the background:
```bash
docker compose up --build -d
```

To view logs:
```bash
docker compose logs -f
```

---

### Step 3 — Deploy on a server (DigitalOcean / AWS / any VPS)

**Get a server (cheapest options):**
| Provider | Plan | Cost | Link |
|----------|------|------|------|
| DigitalOcean | Basic Droplet 1GB | $6/mo | [digitalocean.com](https://digitalocean.com) |
| Hetzner | CX11 | €4/mo | [hetzner.com](https://hetzner.com) |
| Vultr | Cloud Compute 1GB | $6/mo | [vultr.com](https://vultr.com) |

**On the server (Ubuntu 22.04 / 24.04):**

```bash
# 1. Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# 2. Clone your repo
git clone https://github.com/YOUR_USERNAME/lbo-platform.git
cd lbo-platform

# 3. Run on port 80 (no port number in URL)
#    Edit docker-compose.yml first:
#    Change:  "8000:8000"
#    To:      "80:8000"

docker compose up --build -d

# 4. App is live at http://YOUR_SERVER_IP
```

**Check it's running:**
```bash
docker compose ps          # should show "healthy"
docker compose logs -f     # live logs
curl http://localhost/api/health
```

---

### Step 4 — Add a domain name (optional)

If you have a domain (e.g. `lbo.yourdomain.com`):

1. In your DNS provider, add an **A record**:
   - Name: `lbo` (or `@` for root)
   - Value: your server IP
   - TTL: 300

2. Install nginx + certbot for HTTPS:

```bash
sudo apt install nginx certbot python3-certbot-nginx -y

# Create nginx config
sudo nano /etc/nginx/sites-available/lbo

# Paste this (replace lbo.yourdomain.com):
server {
    listen 80;
    server_name lbo.yourdomain.com;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

sudo ln -s /etc/nginx/sites-available/lbo /etc/nginx/sites-enabled/
sudo nginx -t && sudo nginx -s reload

# Enable HTTPS (free SSL)
sudo certbot --nginx -d lbo.yourdomain.com
```

Your app is now at `https://lbo.yourdomain.com` ✓

---

### Updating the app

```bash
git pull                       # pull latest code from GitHub
docker compose up --build -d   # rebuild and restart (zero-downtime swap)
```

---

## File structure

```
lbo-platform/
├── main.py            ← FastAPI app — serves frontend + all API routes
├── lbo_engine.py      ← LBO scoring, deal modeller, sensitivity matrix
├── data_service.py    ← yfinance live fetch + 1-hour cache + seed fallback
├── requirements.txt   ← Python dependencies
├── Dockerfile         ← Multi-stage production Docker build
├── docker-compose.yml ← One-command local + server deployment
├── .dockerignore
├── .gitignore
└── frontend/
    └── index.html     ← Single-file SPA (no build step needed)
```

---

## API endpoints

```
GET  /                              Frontend (HTML)
GET  /docs                          Swagger UI — interactive API explorer

GET  /api/health                    Health check
GET  /api/exchanges                 List all 4 exchanges

GET  /api/companies/{exchange}      Company screener
     ?min_score=75
     &sector=IT+Services
     &max_ev_ebitda=15
     &rating=Strong+Buy
     &search=tcs
     &sort_by=score&sort_dir=desc
     &refresh=true                  Force live yfinance fetch

GET  /api/summary/{exchange}        Metric cards
GET  /api/compare                   Cross-exchange comparison
GET  /api/analytics/{exchange}      Chart datasets

POST /api/model/deal                LBO deal model → IRR, MOIC, schedule
GET  /api/model/sensitivity/{ex}    IRR/MOIC heatmap matrix
```

---

## Exchanges covered

| Exchange | Companies | Universe |
|----------|-----------|---------|
| NASDAQ | 24 | US technology, SaaS, fintech, cybersecurity |
| DOW JONES | 22 | US blue-chip: industrials, healthcare, aerospace |
| NSE / NIFTY 50 | 20 | Indian IT services, software, digital platforms |
| BSE / SENSEX | 20 | Indian FMCG, chemicals, NBFCs, asset management |

Data: `yfinance` live feed with curated seed fallback. Each company tagged `LIVE` or `SEED` in the UI.
