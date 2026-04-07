"""
LBO Engine — scoring model, deal modeller, analytics builder.
"""

from dataclasses import dataclass
from typing import Any
import math


# ── Scoring weights ────────────────────────────────────────────────────────
WEIGHTS = {
    "ebitdaMargin":     20,
    "fcfYield":         18,
    "leverage":         16,
    "roe":              14,
    "roce":             12,
    "evEbitda":         10,
    "revenueGrowth":     6,
    "debtServiceCover":  4,
}


@dataclass
class DealInputs:
    exchange: str          = "nasdaq"
    entry_ev_ebitda: float = 10.0
    ebitda_m: float        = 300.0
    debt_pct: float        = 0.60
    interest_rate: float   = 0.07
    holding_years: int     = 5
    ebitda_growth: float   = 0.08
    exit_ev_ebitda: float  = 9.0
    annual_debt_repay_pct: float = 0.08
    tax_rate: float        = 0.25


class LBOEngine:

    # ── Scoring ────────────────────────────────────────────────────────────

    def score_lbo(self, c: dict) -> int:
        s = 0.0

        # 1. EBITDA margin (0–20)
        #    ≥40% → full marks; scales from 0
        s += min(20.0, (c.get("ebitdaMargin", 0) / 40.0) * 20.0)

        # 2. FCF yield (0–18)
        #    ≥6% → full marks
        s += min(18.0, (c.get("fcfYield", 0) / 6.0) * 18.0)

        # 3. Leverage — debt capacity framing (0–16)
        #    Zero leverage = maximum LBO debt capacity → excellent (score: 14)
        #    Sweet spot: 1.5x–5.0x  → 16 at 3x, decays toward edges
        #    >7x → heavily penalised (high existing debt leaves no room)
        lev = c.get("leverage", 0) or 0
        if lev == 0:
            lev_score = 14.0          # clean balance sheet: prime LBO target
        elif lev <= 1.5:
            lev_score = 14.0 + (lev / 1.5) * 2.0   # 14→16 ramp
        elif lev <= 5.0:
            lev_score = 16.0 - ((lev - 1.5) / 3.5) * 4.0   # 16→12
        elif lev <= 7.0:
            lev_score = 12.0 - ((lev - 5.0) / 2.0) * 8.0   # 12→4
        else:
            lev_score = max(0.0, 4.0 - (lev - 7.0) * 2.0)  # rapid decay
        s += max(0.0, min(16.0, lev_score))

        # 4. ROE (0–14)  — capped at 60% to avoid distortion from debt-inflated ROE
        s += min(14.0, (min(c.get("roe", 0), 60.0) / 60.0) * 14.0)

        # 5. ROCE (0–12)  — capped at 50%
        s += min(12.0, (min(c.get("roce", 0), 50.0) / 50.0) * 12.0)

        # 6. EV/EBITDA entry multiple (0–10)
        #    ≤8x → full score; 8–18x → linear decay; >18x → near-zero
        #    High-quality compounders (BSE/NSE) trade at 30–50x but still have
        #    LBO merit if all other factors are strong — floor at 1.0
        ev_eb = c.get("evEbitda", 0) or 0
        if ev_eb <= 0:
            ev_score = 5.0       # unknown/zero: neutral
        elif ev_eb <= 8:
            ev_score = 10.0
        elif ev_eb <= 18:
            ev_score = 10.0 - (ev_eb - 8) * 0.6   # 10→4
        elif ev_eb <= 40:
            ev_score = 4.0 - (ev_eb - 18) * 0.12  # 4→1.36
        else:
            ev_score = max(1.0, 1.4 - (ev_eb - 40) * 0.02)
        s += max(1.0, ev_score)

        # 7. Revenue growth (0–6)
        s += min(6.0, (min(max(c.get("revenueGrowth", 0), 0), 30.0) / 30.0) * 6.0)

        # 8. Debt service coverage (0–4)
        #    If no debt (dscr=0 because no interest), company can service any new
        #    LBO debt → treat as full score (4.0)
        lev2 = c.get("leverage", 0) or 0
        dscr = c.get("debtServiceCover", 0) or 0
        if lev2 == 0:
            dscr_score = 4.0    # zero debt → perfect serviceability for new LBO debt
        elif dscr >= 3.0:
            dscr_score = 4.0
        elif dscr >= 1.5:
            dscr_score = 4.0 * ((dscr - 1.5) / 1.5)
        else:
            dscr_score = max(0.0, dscr / 1.5 * 2.0)
        s += dscr_score

        return int(min(99, max(20, round(s))))

    def get_rating(self, score: int) -> str:
        if score >= 80: return "Strong Buy"
        if score >= 70: return "Buy"
        if score >= 55: return "Watch"
        return "Pass"

    # ── Summary ────────────────────────────────────────────────────────────

    def compute_summary(self, companies: list[dict]) -> dict:
        if not companies:
            return {}

        def avg(key):
            vals = [c.get(key, 0) or 0 for c in companies]
            return round(sum(vals) / len(vals), 1)

        strong_buy = [c for c in companies if c.get("score", 0) >= 80]
        buy        = [c for c in companies if c.get("score", 0) >= 70]

        return {
            "total":           len(companies),
            "strongBuy":       len(strong_buy),
            "buy":             len(buy),
            "avgScore":        avg("score"),
            "avgEbitdaMargin": avg("ebitdaMargin"),
            "avgFcfYield":     avg("fcfYield"),
            "avgLeverage":     avg("leverage"),
            "avgRoe":          avg("roe"),
            "avgRoce":         avg("roce"),
            "avgEvEbitda":     avg("evEbitda"),
            "avgRevGrowth":    avg("revenueGrowth"),
        }

    # ── Deal Model ─────────────────────────────────────────────────────────

    def run_deal_model(self, inp: DealInputs) -> dict:
        ev          = inp.entry_ev_ebitda * inp.ebitda_m
        total_debt  = ev * inp.debt_pct
        equity_in   = ev * (1 - inp.debt_pct)
        debt        = total_debt
        schedule    = []

        for yr in range(1, inp.holding_years + 1):
            ebitda_yr  = inp.ebitda_m * (1 + inp.ebitda_growth) ** yr
            interest   = debt * inp.interest_rate
            repaid     = min(debt, total_debt * inp.annual_debt_repay_pct)
            debt       = max(0.0, debt - repaid)
            fcf_proxy  = ebitda_yr * 0.75 * (1 - inp.tax_rate)
            dscr       = round(ebitda_yr / max(interest, 1), 2)
            nd_ebitda  = round(debt / max(ebitda_yr, 1), 2)

            schedule.append({
                "year":         yr,
                "ebitda":       round(ebitda_yr),
                "interest":     round(interest),
                "debtRepaid":   round(repaid),
                "remainingDebt":round(debt),
                "fcfProxy":     round(fcf_proxy),
                "dscr":         dscr,
                "ndEbitda":     nd_ebitda,
            })

        exit_debt   = schedule[-1]["remainingDebt"]
        exit_ebitda = inp.ebitda_m * (1 + inp.ebitda_growth) ** inp.holding_years
        exit_ev     = inp.exit_ev_ebitda * exit_ebitda
        exit_equity = exit_ev - exit_debt
        moic        = exit_equity / max(equity_in, 1)
        irr_raw     = math.pow(max(moic, 0.01), 1 / inp.holding_years) - 1

        debt_paydown        = total_debt - exit_debt
        ebitda_growth_gain  = inp.exit_ev_ebitda * (exit_ebitda - inp.ebitda_m)
        multiple_expansion  = (inp.exit_ev_ebitda - inp.entry_ev_ebitda) * exit_ebitda

        return {
            "summary": {
                "entryEV":          round(ev),
                "equityIn":         round(equity_in),
                "totalDebt":        round(total_debt),
                "exitEV":           round(exit_ev),
                "exitEquity":       round(exit_equity),
                "moic":             round(moic, 2),
                "irr":              round(irr_raw * 100, 1),
                "debtPaydown":      round(debt_paydown),
                "exitEbitda":       round(exit_ebitda),
                "exitDebt":         round(exit_debt),
            },
            "bridge": {
                "equityIn":          round(equity_in),
                "ebitdaGrowth":      round(ebitda_growth_gain),
                "debtPaydown":       round(debt_paydown),
                "multipleExpansion": round(multiple_expansion - ebitda_growth_gain),
                "equityOut":         round(exit_equity),
            },
            "schedule": schedule,
        }

    # ── Sensitivity matrix ─────────────────────────────────────────────────

    def sensitivity_matrix(self, ebitda_m: float, debt_pct: float, holding_years: int) -> dict:
        entries = [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 14.0]
        growths = [0.04, 0.06, 0.08, 0.10, 0.12, 0.15]

        irr_matrix = []
        moic_matrix = []

        for g in growths:
            irr_row, moic_row = [], []
            for e in entries:
                inp = DealInputs(
                    entry_ev_ebitda=e, ebitda_m=ebitda_m,
                    debt_pct=debt_pct, interest_rate=0.07,
                    holding_years=holding_years, ebitda_growth=g,
                    exit_ev_ebitda=e * 0.9, annual_debt_repay_pct=0.08,
                    tax_rate=0.25,
                )
                res = self.run_deal_model(inp)
                irr_row.append(res["summary"]["irr"])
                moic_row.append(res["summary"]["moic"])
            irr_matrix.append(irr_row)
            moic_matrix.append(moic_row)

        return {
            "entryMultiples": entries,
            "growthRates":    [f"{int(g*100)}%" for g in growths],
            "irrMatrix":      irr_matrix,
            "moicMatrix":     moic_matrix,
        }

    # ── Analytics ─────────────────────────────────────────────────────────

    def compute_analytics(self, companies: list[dict], exchange: str) -> dict:
        sectors = list(set(c["sector"] for c in companies))

        sector_avg_ebitda = {}
        for sec in sectors:
            grp = [c for c in companies if c["sector"] == sec]
            sector_avg_ebitda[sec] = round(
                sum(c.get("ebitdaMargin", 0) for c in grp) / len(grp), 1
            )

        score_buckets = {
            "lt55":   len([c for c in companies if c.get("score", 0) < 55]),
            "55to64": len([c for c in companies if 55 <= c.get("score", 0) < 65]),
            "65to74": len([c for c in companies if 65 <= c.get("score", 0) < 75]),
            "75to84": len([c for c in companies if 75 <= c.get("score", 0) < 85]),
            "gte85":  len([c for c in companies if c.get("score", 0) >= 85]),
        }

        scatter_roe_roce = [
            {"x": c.get("roe", 0), "y": c.get("roce", 0),
             "name": c["name"], "score": c.get("score", 0)}
            for c in companies
        ]

        scatter_lev_fcf = [
            {"x": c.get("leverage", 0), "y": c.get("fcfYield", 0),
             "name": c["name"], "score": c.get("score", 0)}
            for c in companies
        ]

        return {
            "exchange":          exchange,
            "sectorEbitdaAvg":   sector_avg_ebitda,
            "scoreBuckets":      score_buckets,
            "scatterRoeRoce":    scatter_roe_roce,
            "scatterLevFcf":     scatter_lev_fcf,
            "evEbitdaList":      [{"ticker": c["ticker"], "value": c.get("evEbitda", 0)} for c in companies],
            "revGrowthList":     [{"ticker": c["ticker"], "value": c.get("revenueGrowth", 0)} for c in companies],
        }
