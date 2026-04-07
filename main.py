"""
LBO Intelligence Platform — FastAPI Backend (Production)
Serves the frontend AND provides all API endpoints from one process.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional
import os, re

from data_service import DataService
from lbo_engine import LBOEngine, DealInputs

app = FastAPI(title="LBO Intelligence API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

data_svc  = DataService()
lbo_engine = LBOEngine()

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend")

# ── Serve frontend at root — rewrites API_BASE to relative path ──────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    idx = os.path.join(FRONTEND_DIR, "index.html")
    if not os.path.exists(idx):
        return HTMLResponse("<h2>Frontend not found. Place frontend/index.html next to main.py</h2>", status_code=404)
    html = open(idx).read()
    # Rewrite API_BASE so frontend talks to the same origin — works on any host/port
    html = re.sub(
        r"const API_BASE\s*=\s*['\"].*?['\"];",
        "const API_BASE = '';",
        html
    )
    return HTMLResponse(html)

# Serve static assets (CSS, JS, images) if any are separate files
if os.path.exists(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")


# ─────────────────────────────────────────
#  EXCHANGE ENDPOINTS
# ─────────────────────────────────────────

@app.get("/api/exchanges")
async def list_exchanges():
    return {
        "exchanges": [
            {"id":"nasdaq","name":"NASDAQ",         "badge":"NYSE · USD","currency":"USD","symbol":"$", "color":"#4a8fff"},
            {"id":"dow",   "name":"DOW JONES",      "badge":"NYSE · USD","currency":"USD","symbol":"$", "color":"#c9a84c"},
            {"id":"nse",   "name":"NSE / NIFTY 50", "badge":"NSE · INR", "currency":"INR","symbol":"₹","color":"#2ecc8a"},
            {"id":"bse",   "name":"BSE / SENSEX",   "badge":"BSE · INR", "currency":"INR","symbol":"₹","color":"#e05555"},
        ]
    }


@app.get("/api/companies/{exchange}")
async def get_companies(
    exchange: str,
    sector:   Optional[str]   = None,
    min_score: int             = 0,
    max_ev_ebitda: float       = 99,
    rating:   Optional[str]   = None,
    search:   Optional[str]   = None,
    sort_by:  str              = "score",
    sort_dir: str              = "desc",
    refresh:  bool             = False,
):
    if exchange not in ("nasdaq","dow","nse","bse"):
        raise HTTPException(status_code=404, detail="Exchange not found")

    companies = await data_svc.get_exchange_data(exchange, refresh=refresh)

    if sector:      companies = [c for c in companies if c["sector"].lower() == sector.lower()]
    if min_score:   companies = [c for c in companies if c["score"] >= min_score]
    if max_ev_ebitda < 99: companies = [c for c in companies if c["evEbitda"] <= max_ev_ebitda]
    if rating:      companies = [c for c in companies if c["rating"] == rating]
    if search:
        s = search.lower()
        companies = [c for c in companies if s in c["name"].lower() or s in c["ticker"].lower()]

    rev = sort_dir == "desc"
    try:
        companies.sort(key=lambda c: (c.get(sort_by) or 0), reverse=rev)
    except Exception:
        pass

    return {"exchange": exchange, "count": len(companies), "companies": companies}


@app.get("/api/companies/{exchange}/sectors")
async def get_sectors(exchange: str):
    companies = await data_svc.get_exchange_data(exchange)
    return {"sectors": sorted(set(c["sector"] for c in companies))}


@app.get("/api/summary/{exchange}")
async def get_summary(exchange: str):
    companies = await data_svc.get_exchange_data(exchange)
    if not companies:
        raise HTTPException(status_code=404, detail="No data")
    return lbo_engine.compute_summary(companies)


@app.get("/api/compare")
async def compare_exchanges():
    result = {}
    for ex in ("nasdaq","dow","nse","bse"):
        companies = await data_svc.get_exchange_data(ex)
        result[ex] = lbo_engine.compute_summary(companies)
    return result


@app.get("/api/analytics/{exchange}")
async def analytics(exchange: str):
    companies = await data_svc.get_exchange_data(exchange)
    return lbo_engine.compute_analytics(companies, exchange)


# ─────────────────────────────────────────
#  DEAL MODEL
# ─────────────────────────────────────────

class DealRequest(BaseModel):
    exchange:             str   = "nasdaq"
    entry_ev_ebitda:      float = 10.0
    ebitda_m:             float = 300.0
    debt_pct:             float = 0.60
    interest_rate:        float = 0.07
    holding_years:        int   = 5
    ebitda_growth:        float = 0.08
    exit_ev_ebitda:       float = 9.0
    annual_debt_repay_pct:float = 0.08
    tax_rate:             float = 0.25


@app.post("/api/model/deal")
async def run_deal_model(req: DealRequest):
    return lbo_engine.run_deal_model(DealInputs(**req.dict()))


@app.get("/api/model/sensitivity/{exchange}")
async def sensitivity(exchange: str, ebitda_m: float = 300, debt_pct: float = 0.60, holding_years: int = 5):
    return lbo_engine.sensitivity_matrix(ebitda_m, debt_pct, holding_years)


# ─────────────────────────────────────────
#  HEALTH
# ─────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "cache_keys": list(data_svc._cache.keys())}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
