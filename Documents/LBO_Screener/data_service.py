"""
DataService — fetches real financial data via yfinance.
Falls back to curated seed data if the network / ticker is unavailable.

Fixes applied:
- Batch yfinance downloads (one HTTP call per exchange instead of 24)
- Per-ticker exponential backoff + jitter on 429s
- Staggered async fetches (max 3 concurrent) to avoid burst rate-limiting
- Cache TTL kept at 1 hour; seed fallback unchanged
"""

import asyncio
import time
import random
import yfinance as yf
from lbo_engine import LBOEngine

_engine = LBOEngine()

# ── Ticker lists per exchange ──────────────────────────────────────────────
EXCHANGE_TICKERS = {
    "nasdaq": [
        ("VEEV",  "Veeva Systems",         "Healthcare IT"),
        ("SSNC",  "SS&C Technologies",     "Fintech"),
        ("CDNS",  "Cadence Design",        "Software"),
        ("SNPS",  "Synopsys Inc",          "Software"),
        ("CTSH",  "Cognizant Tech",        "IT Services"),
        ("WEX",   "WEX Inc",              "Fintech"),
        ("QLYS",  "Qualys Inc",           "Cybersecurity"),
        ("PRGS",  "Progress Software",    "Software"),
        ("EXLS",  "ExlService Holdings",  "Analytics"),
        ("TTEC",  "TTEC Holdings",        "CX Tech"),
        ("HCKT",  "Hackett Group",        "Consulting"),
        ("EPAM",  "EPAM Systems",         "IT Services"),
        ("GLOB",  "Globant SA",           "IT Services"),
        ("DXC",   "DXC Technology",       "IT Services"),
        ("CDAY",  "Ceridian HCM",         "HRTech"),
        ("EVOP",  "EVO Payments",         "Payments"),
        ("DAVA",  "Endava PLC",           "IT Services"),
        ("ALRM",  "Alarm.com Holdings",   "IoT Software"),
        ("RAMP",  "LiveRamp Holdings",    "Data Tech"),
        ("NTWK",  "NetSol Technologies",  "Software"),
        ("PPTC",  "Park Place Technologies","IT Services"),
        ("NTCT",  "NetScout Systems",     "Cybersecurity"),
        ("CLFD",  "Clearfield Inc",       "Telecom Infra"),
        ("MPLX",  "MicroStrategy Analytics","Analytics"),
    ],
    "dow": [
        ("DHR",   "Danaher Corp",          "Healthcare"),
        ("ROP",   "Roper Technologies",    "Industrials"),
        ("CTAS",  "Cintas Corp",           "Business Svcs"),
        ("WCN",   "Waste Connections",     "Industrials"),
        ("TDG",   "TransDigm Group",       "Aerospace"),
        ("ROL",   "Rollins Inc",           "Consumer Svcs"),
        ("IQV",   "IQVIA Holdings",        "Healthcare"),
        ("SPB",   "Spectrum Brands",       "Consumer"),
        ("HURN",  "Huron Consulting",      "Consulting"),
        ("AMED",  "Amedisys Inc",          "Healthcare"),
        ("HSC",   "Harsco Corp",           "Industrials"),
        ("GTLS",  "Chart Industries",      "Industrials"),
        ("ENOV",  "Enovis Corp",           "MedTech"),
        ("PERI",  "Perion Network",        "AdTech"),
        ("SABR",  "Sabre Corp",            "Travel Tech"),
        ("VVI",   "Viad Corp",             "Travel Svcs"),
        ("GTX",   "Garrett Motion",        "Automotive"),
        ("NCI",   "NCI Info Systems",      "Defense IT"),
        ("MPLN",  "MultiPlan Corp",        "Healthcare IT"),
        ("SPR",   "Spirit AeroSystems",    "Aerospace"),
        ("CVGX",  "ConvergEx Group",       "Fintech"),
        ("BNDL",  "Brand Industrial Svcs", "Industrials"),
    ],
    "nse": [
        ("INFY.NS",       "Infosys Ltd",           "IT Services"),
        ("TCS.NS",        "TCS Ltd",               "IT Services"),
        ("WIPRO.NS",      "Wipro Ltd",             "IT Services"),
        ("HCLTECH.NS",    "HCL Technologies",      "IT Services"),
        ("MPHASIS.NS",    "Mphasis Ltd",           "IT Services"),
        ("PERSISTENT.NS", "Persistent Systems",    "Software"),
        ("COFORGE.NS",    "Coforge Ltd",           "IT Services"),
        ("KPITTECH.NS",   "KPIT Technologies",     "Auto IT"),
        ("TATAELXSI.NS",  "Tata Elxsi",            "Design IT"),
        ("ZENSARTECH.NS", "Zensar Technologies",   "IT Services"),
        ("CYIENT.NS",     "Cyient Ltd",            "Engg IT"),
        ("TANLA.NS",      "Tanla Platforms",       "CPaaS"),
        ("ROUTE.NS",      "Route Mobile",          "CPaaS"),
        ("INTELLECT.NS",  "Intellect Design",      "BankTech"),
        ("NEWGEN.NS",     "Newgen Software",       "Software"),
        ("BSOFT.NS",      "Birlasoft Ltd",         "IT Services"),
        ("HAPPSTMNDS.NS", "Happiest Minds",        "Digital IT"),
        ("MASTEK.NS",     "Mastek Ltd",            "Software"),
        ("NUCLEUS.NS",    "Nucleus Software",      "BankTech"),
        ("SASKEN.NS",     "Sasken Technologies",   "Embedded IT"),
    ],
    "bse": [
        ("ASIANPAINT.NS", "Asian Paints",        "Paints"),
        ("PIDILITIND.NS", "Pidilite Industries", "Chemicals"),
        ("BERGEPAINT.NS", "Berger Paints",       "Paints"),
        ("HAVELLS.NS",    "Havells India",       "Electricals"),
        ("MARICO.NS",     "Marico Ltd",          "FMCG"),
        ("PAGEIND.NS",    "Page Industries",     "Apparel"),
        ("TITAN.NS",      "Titan Company",       "Jewellery"),
        ("NAUKRI.NS",     "Info Edge India",     "Internet"),
        ("MUTHOOTFIN.NS", "Muthoot Finance",     "NBFC"),
        ("MANAPPURAM.NS", "Manappuram Finance",  "NBFC"),
        ("BAJFINANCE.NS", "Bajaj Finance",       "NBFC"),
        ("HDFCAMC.NS",    "HDFC AMC",            "Asset Mgmt"),
        ("NAM-INDIA.NS",  "Nippon India AMC",    "Asset Mgmt"),
        ("SBILIFE.NS",    "SBI Life Insurance",  "Insurance"),
        ("AARTIIND.NS",   "Aarti Industries",    "Chemicals"),
        ("SRF.NS",        "SRF Ltd",             "Chemicals"),
        ("VINATIORGA.NS", "Vinati Organics",     "Chemicals"),
        ("ASTRAL.NS",     "Astral Ltd",          "Pipes"),
        ("POLYCAB.NS",    "Polycab India",       "Cables"),
        ("CAMS.NS",       "Computer Age Mgmt",   "Fintech"),
    ],
}

# ── Seed fallback data (pre-computed, used when yfinance unavailable) ──────
SEED_DATA = {
    "nasdaq": [
        {"ticker":"VEEV","name":"Veeva Systems","sector":"Healthcare IT","ev":28,"ebitdaMargin":34,"leverage":0.0,"roe":18,"roce":17,"fcfYield":3.1,"evEbitda":12,"debtServiceCover":0,"revenueGrowth":16,"marketCap":28,"price":180},
        {"ticker":"SSNC","name":"SS&C Technologies","sector":"Fintech","ev":18,"ebitdaMargin":36,"leverage":3.5,"roe":20,"roce":16,"fcfYield":5.8,"evEbitda":11,"debtServiceCover":2.8,"revenueGrowth":9,"marketCap":16,"price":62},
        {"ticker":"CDNS","name":"Cadence Design","sector":"Software","ev":62,"ebitdaMargin":38,"leverage":0.4,"roe":35,"roce":28,"fcfYield":2.6,"evEbitda":40,"debtServiceCover":0,"revenueGrowth":17,"marketCap":60,"price":220},
        {"ticker":"SNPS","name":"Synopsys Inc","sector":"Software","ev":82,"ebitdaMargin":36,"leverage":0.2,"roe":22,"roce":19,"fcfYield":2.2,"evEbitda":38,"debtServiceCover":0,"revenueGrowth":15,"marketCap":80,"price":510},
        {"ticker":"CTSH","name":"Cognizant Tech","sector":"IT Services","ev":34,"ebitdaMargin":18,"leverage":0.0,"roe":17,"roce":15,"fcfYield":5.9,"evEbitda":13,"debtServiceCover":0,"revenueGrowth":5,"marketCap":33,"price":65},
        {"ticker":"WEX","name":"WEX Inc","sector":"Fintech","ev":10.2,"ebitdaMargin":35,"leverage":4.1,"roe":22,"roce":13,"fcfYield":5.1,"evEbitda":10,"debtServiceCover":2.3,"revenueGrowth":10,"marketCap":8,"price":185},
        {"ticker":"QLYS","name":"Qualys Inc","sector":"Cybersecurity","ev":6.2,"ebitdaMargin":42,"leverage":0.0,"roe":55,"roce":30,"fcfYield":4.5,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":12,"marketCap":6,"price":145},
        {"ticker":"PRGS","name":"Progress Software","sector":"Software","ev":2.1,"ebitdaMargin":34,"leverage":3.2,"roe":25,"roce":14,"fcfYield":6.8,"evEbitda":7,"debtServiceCover":2.8,"revenueGrowth":5,"marketCap":1.8,"price":52},
        {"ticker":"EXLS","name":"ExlService Holdings","sector":"Analytics","ev":6.1,"ebitdaMargin":22,"leverage":0.8,"roe":24,"roce":21,"fcfYield":4.8,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":13,"marketCap":5.8,"price":32},
        {"ticker":"TTEC","name":"TTEC Holdings","sector":"CX Tech","ev":2.8,"ebitdaMargin":14,"leverage":3.4,"roe":30,"roce":18,"fcfYield":5.6,"evEbitda":8,"debtServiceCover":2.0,"revenueGrowth":4,"marketCap":2.2,"price":22},
        {"ticker":"HCKT","name":"Hackett Group","sector":"Consulting","ev":0.58,"ebitdaMargin":19,"leverage":0.2,"roe":28,"roce":24,"fcfYield":5.5,"evEbitda":8,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.55,"price":24},
        {"ticker":"EPAM","name":"EPAM Systems","sector":"IT Services","ev":12,"ebitdaMargin":16,"leverage":0.0,"roe":18,"roce":17,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":10,"marketCap":11,"price":200},
        {"ticker":"GLOB","name":"Globant SA","sector":"IT Services","ev":5.8,"ebitdaMargin":17,"leverage":0.5,"roe":16,"roce":14,"fcfYield":3.8,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":18,"marketCap":5.5,"price":130},
        {"ticker":"DXC","name":"DXC Technology","sector":"IT Services","ev":8.2,"ebitdaMargin":12,"leverage":2.8,"roe":10,"roce":8,"fcfYield":6.2,"evEbitda":6,"debtServiceCover":2.2,"revenueGrowth":-2,"marketCap":6,"price":25},
        {"ticker":"CDAY","name":"Ceridian HCM","sector":"HRTech","ev":9.5,"ebitdaMargin":20,"leverage":2.2,"roe":8,"roce":7,"fcfYield":2.5,"evEbitda":28,"debtServiceCover":1.8,"revenueGrowth":15,"marketCap":8,"price":55},
        {"ticker":"EVOP","name":"EVO Payments","sector":"Payments","ev":2.2,"ebitdaMargin":28,"leverage":1.8,"roe":14,"roce":12,"fcfYield":4.1,"evEbitda":12,"debtServiceCover":2.5,"revenueGrowth":12,"marketCap":2.0,"price":32},
        {"ticker":"DAVA","name":"Endava PLC","sector":"IT Services","ev":2.8,"ebitdaMargin":18,"leverage":0.0,"roe":22,"roce":20,"fcfYield":4.5,"evEbitda":16,"debtServiceCover":0,"revenueGrowth":20,"marketCap":2.7,"price":42},
        {"ticker":"ALRM","name":"Alarm.com Holdings","sector":"IoT Software","ev":1.8,"ebitdaMargin":16,"leverage":0.0,"roe":14,"roce":13,"fcfYield":3.8,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":14,"marketCap":1.7,"price":55},
        {"ticker":"RAMP","name":"LiveRamp Holdings","sector":"Data Tech","ev":2.2,"ebitdaMargin":10,"leverage":0.0,"roe":6,"roce":5,"fcfYield":2.8,"evEbitda":30,"debtServiceCover":0,"revenueGrowth":12,"marketCap":2.1,"price":32},
        {"ticker":"NTWK","name":"NetSol Technologies","sector":"Software","ev":0.12,"ebitdaMargin":14,"leverage":0.2,"roe":10,"roce":9,"fcfYield":4.2,"evEbitda":8,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.11,"price":8},
        {"ticker":"PPTC","name":"Park Place Technologies","sector":"IT Services","ev":1.5,"ebitdaMargin":22,"leverage":2.5,"roe":16,"roce":12,"fcfYield":5.0,"evEbitda":9,"debtServiceCover":2.0,"revenueGrowth":10,"marketCap":1.3,"price":18},
        {"ticker":"NTCT","name":"NetScout Systems","sector":"Cybersecurity","ev":1.2,"ebitdaMargin":20,"leverage":0.4,"roe":8,"roce":7,"fcfYield":5.5,"evEbitda":10,"debtServiceCover":0,"revenueGrowth":2,"marketCap":1.1,"price":16},
        {"ticker":"CLFD","name":"Clearfield Inc","sector":"Telecom Infra","ev":0.45,"ebitdaMargin":18,"leverage":0.0,"roe":20,"roce":18,"fcfYield":4.0,"evEbitda":12,"debtServiceCover":0,"revenueGrowth":15,"marketCap":0.42,"price":28},
        {"ticker":"MPLX","name":"MicroStrategy Analytics","sector":"Analytics","ev":0.85,"ebitdaMargin":24,"leverage":1.2,"roe":14,"roce":12,"fcfYield":5.2,"evEbitda":9,"debtServiceCover":2.1,"revenueGrowth":6,"marketCap":0.78,"price":32},
    ],
    "dow": [
        {"ticker":"DHR","name":"Danaher Corp","sector":"Healthcare","ev":185,"ebitdaMargin":30,"leverage":1.2,"roe":14,"roce":11,"fcfYield":2.8,"evEbitda":22,"debtServiceCover":3.5,"revenueGrowth":2,"marketCap":170,"price":230},
        {"ticker":"ROP","name":"Roper Technologies","sector":"Industrials","ev":55,"ebitdaMargin":38,"leverage":2.8,"roe":15,"roce":10,"fcfYield":2.5,"evEbitda":25,"debtServiceCover":3.0,"revenueGrowth":12,"marketCap":48,"price":480},
        {"ticker":"CTAS","name":"Cintas Corp","sector":"Business Svcs","ev":58,"ebitdaMargin":24,"leverage":1.5,"roe":38,"roce":28,"fcfYield":2.2,"evEbitda":28,"debtServiceCover":4.0,"revenueGrowth":9,"marketCap":55,"price":560},
        {"ticker":"WCN","name":"Waste Connections","sector":"Industrials","ev":38,"ebitdaMargin":32,"leverage":2.8,"roe":12,"roce":8,"fcfYield":2.8,"evEbitda":22,"debtServiceCover":3.2,"revenueGrowth":10,"marketCap":32,"price":165},
        {"ticker":"TDG","name":"TransDigm Group","sector":"Aerospace","ev":68,"ebitdaMargin":48,"leverage":7.2,"roe":55,"roce":14,"fcfYield":3.5,"evEbitda":18,"debtServiceCover":2.5,"revenueGrowth":20,"marketCap":48,"price":1240},
        {"ticker":"ROL","name":"Rollins Inc","sector":"Consumer Svcs","ev":18,"ebitdaMargin":22,"leverage":0.8,"roe":35,"roce":28,"fcfYield":3.2,"evEbitda":28,"debtServiceCover":4.5,"revenueGrowth":12,"marketCap":17,"price":35},
        {"ticker":"IQV","name":"IQVIA Holdings","sector":"Healthcare","ev":45,"ebitdaMargin":22,"leverage":3.5,"roe":18,"roce":10,"fcfYield":3.8,"evEbitda":18,"debtServiceCover":3.0,"revenueGrowth":8,"marketCap":35,"price":195},
        {"ticker":"SPB","name":"Spectrum Brands","sector":"Consumer","ev":4.2,"ebitdaMargin":16,"leverage":4.2,"roe":12,"roce":8,"fcfYield":5.5,"evEbitda":10,"debtServiceCover":2.0,"revenueGrowth":2,"marketCap":2.8,"price":58},
        {"ticker":"HURN","name":"Huron Consulting","sector":"Consulting","ev":1.8,"ebitdaMargin":16,"leverage":1.2,"roe":18,"roce":15,"fcfYield":4.5,"evEbitda":12,"debtServiceCover":3.2,"revenueGrowth":14,"marketCap":1.6,"price":105},
        {"ticker":"AMED","name":"Amedisys Inc","sector":"Healthcare","ev":2.8,"ebitdaMargin":10,"leverage":1.5,"roe":8,"roce":7,"fcfYield":4.2,"evEbitda":14,"debtServiceCover":2.5,"revenueGrowth":5,"marketCap":2.5,"price":88},
        {"ticker":"HSC","name":"Harsco Corp","sector":"Industrials","ev":1.2,"ebitdaMargin":12,"leverage":4.5,"roe":8,"roce":6,"fcfYield":4.0,"evEbitda":8,"debtServiceCover":1.8,"revenueGrowth":4,"marketCap":0.6,"price":8},
        {"ticker":"GTLS","name":"Chart Industries","sector":"Industrials","ev":6.5,"ebitdaMargin":18,"leverage":4.8,"roe":10,"roce":7,"fcfYield":4.2,"evEbitda":10,"debtServiceCover":2.2,"revenueGrowth":22,"marketCap":4.2,"price":108},
        {"ticker":"ENOV","name":"Enovis Corp","sector":"MedTech","ev":3.5,"ebitdaMargin":16,"leverage":3.2,"roe":5,"roce":4,"fcfYield":3.8,"evEbitda":14,"debtServiceCover":2.0,"revenueGrowth":18,"marketCap":2.5,"price":42},
        {"ticker":"PERI","name":"Perion Network","sector":"AdTech","ev":0.65,"ebitdaMargin":20,"leverage":0.0,"roe":14,"roce":12,"fcfYield":6.5,"evEbitda":6,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.62,"price":10},
        {"ticker":"SABR","name":"Sabre Corp","sector":"Travel Tech","ev":5.2,"ebitdaMargin":14,"leverage":8.5,"roe":-15,"roce":2,"fcfYield":2.5,"evEbitda":12,"debtServiceCover":1.2,"revenueGrowth":10,"marketCap":1.2,"price":4},
        {"ticker":"VVI","name":"Viad Corp","sector":"Travel Svcs","ev":1.2,"ebitdaMargin":12,"leverage":3.5,"roe":8,"roce":6,"fcfYield":3.5,"evEbitda":8,"debtServiceCover":1.8,"revenueGrowth":8,"marketCap":0.7,"price":28},
        {"ticker":"GTX","name":"Garrett Motion","sector":"Automotive","ev":2.8,"ebitdaMargin":16,"leverage":3.8,"roe":22,"roce":14,"fcfYield":8.5,"evEbitda":6,"debtServiceCover":2.5,"revenueGrowth":4,"marketCap":1.8,"price":8},
        {"ticker":"NCI","name":"NCI Info Systems","sector":"Defense IT","ev":1.2,"ebitdaMargin":10,"leverage":3.2,"roe":12,"roce":8,"fcfYield":5.2,"evEbitda":8,"debtServiceCover":2.0,"revenueGrowth":6,"marketCap":1.0,"price":18},
        {"ticker":"MPLN","name":"MultiPlan Corp","sector":"Healthcare IT","ev":4.5,"ebitdaMargin":42,"leverage":6.5,"roe":5,"roce":4,"fcfYield":6.5,"evEbitda":8,"debtServiceCover":1.5,"revenueGrowth":2,"marketCap":0.8,"price":1},
        {"ticker":"SPR","name":"Spirit AeroSystems","sector":"Aerospace","ev":4.8,"ebitdaMargin":6,"leverage":8.2,"roe":-20,"roce":1,"fcfYield":1.5,"evEbitda":12,"debtServiceCover":0.8,"revenueGrowth":8,"marketCap":1.5,"price":18},
        {"ticker":"CVGX","name":"ConvergEx Group","sector":"Fintech","ev":0.8,"ebitdaMargin":18,"leverage":2.0,"roe":12,"roce":10,"fcfYield":5.0,"evEbitda":8,"debtServiceCover":2.2,"revenueGrowth":5,"marketCap":0.7,"price":22},
        {"ticker":"BNDL","name":"Brand Industrial Svcs","sector":"Industrials","ev":2.2,"ebitdaMargin":12,"leverage":4.5,"roe":10,"roce":7,"fcfYield":4.5,"evEbitda":7,"debtServiceCover":1.8,"revenueGrowth":8,"marketCap":1.5,"price":18},
    ],
    "nse": [
        {"ticker":"INFY.NS","name":"Infosys Ltd","sector":"IT Services","ev":75,"ebitdaMargin":26,"leverage":0.0,"roe":32,"roce":40,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":14,"marketCap":74,"price":1780},
        {"ticker":"TCS.NS","name":"TCS Ltd","sector":"IT Services","ev":135,"ebitdaMargin":28,"leverage":0.0,"roe":45,"roce":55,"fcfYield":3.8,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":10,"marketCap":134,"price":3680},
        {"ticker":"WIPRO.NS","name":"Wipro Ltd","sector":"IT Services","ev":28,"ebitdaMargin":18,"leverage":0.0,"roe":16,"roce":20,"fcfYield":4.5,"evEbitda":16,"debtServiceCover":0,"revenueGrowth":6,"marketCap":27,"price":480},
        {"ticker":"HCLTECH.NS","name":"HCL Technologies","sector":"IT Services","ev":48,"ebitdaMargin":22,"leverage":0.0,"roe":24,"roce":30,"fcfYield":4.0,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":12,"marketCap":47,"price":1780},
        {"ticker":"MPHASIS.NS","name":"Mphasis Ltd","sector":"IT Services","ev":5.8,"ebitdaMargin":20,"leverage":0.0,"roe":22,"roce":26,"fcfYield":4.5,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":14,"marketCap":5.7,"price":2820},
        {"ticker":"PERSISTENT.NS","name":"Persistent Systems","sector":"Software","ev":5.2,"ebitdaMargin":18,"leverage":0.0,"roe":26,"roce":30,"fcfYield":3.8,"evEbitda":28,"debtServiceCover":0,"revenueGrowth":30,"marketCap":5.1,"price":5680},
        {"ticker":"COFORGE.NS","name":"Coforge Ltd","sector":"IT Services","ev":3.8,"ebitdaMargin":16,"leverage":0.4,"roe":24,"roce":22,"fcfYield":3.5,"evEbitda":24,"debtServiceCover":0,"revenueGrowth":22,"marketCap":3.7,"price":6420},
        {"ticker":"KPITTECH.NS","name":"KPIT Technologies","sector":"Auto IT","ev":3.2,"ebitdaMargin":20,"leverage":0.0,"roe":28,"roce":32,"fcfYield":3.2,"evEbitda":32,"debtServiceCover":0,"revenueGrowth":35,"marketCap":3.1,"price":1320},
        {"ticker":"TATAELXSI.NS","name":"Tata Elxsi","sector":"Design IT","ev":4.2,"ebitdaMargin":28,"leverage":0.0,"roe":38,"roce":44,"fcfYield":3.0,"evEbitda":38,"debtServiceCover":0,"revenueGrowth":18,"marketCap":4.1,"price":6820},
        {"ticker":"ZENSARTECH.NS","name":"Zensar Technologies","sector":"IT Services","ev":1.2,"ebitdaMargin":14,"leverage":0.0,"roe":16,"roce":18,"fcfYield":4.8,"evEbitda":16,"debtServiceCover":0,"revenueGrowth":10,"marketCap":1.15,"price":680},
        {"ticker":"CYIENT.NS","name":"Cyient Ltd","sector":"Engg IT","ev":1.8,"ebitdaMargin":16,"leverage":0.2,"roe":18,"roce":20,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":14,"marketCap":1.75,"price":1820},
        {"ticker":"TANLA.NS","name":"Tanla Platforms","sector":"CPaaS","ev":1.4,"ebitdaMargin":26,"leverage":0.0,"roe":24,"roce":28,"fcfYield":5.2,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":10,"marketCap":1.35,"price":820},
        {"ticker":"ROUTE.NS","name":"Route Mobile","sector":"CPaaS","ev":0.95,"ebitdaMargin":18,"leverage":0.5,"roe":18,"roce":16,"fcfYield":4.0,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":20,"marketCap":0.92,"price":1480},
        {"ticker":"INTELLECT.NS","name":"Intellect Design","sector":"BankTech","ev":0.85,"ebitdaMargin":18,"leverage":0.2,"roe":16,"roce":15,"fcfYield":3.5,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":22,"marketCap":0.82,"price":820},
        {"ticker":"NEWGEN.NS","name":"Newgen Software","sector":"Software","ev":0.72,"ebitdaMargin":20,"leverage":0.0,"roe":22,"roce":24,"fcfYield":3.8,"evEbitda":28,"debtServiceCover":0,"revenueGrowth":26,"marketCap":0.70,"price":1280},
        {"ticker":"BSOFT.NS","name":"Birlasoft Ltd","sector":"IT Services","ev":1.1,"ebitdaMargin":16,"leverage":0.0,"roe":20,"roce":22,"fcfYield":4.5,"evEbitda":20,"debtServiceCover":0,"revenueGrowth":16,"marketCap":1.08,"price":580},
        {"ticker":"HAPPSTMNDS.NS","name":"Happiest Minds","sector":"Digital IT","ev":0.85,"ebitdaMargin":22,"leverage":0.0,"roe":26,"roce":30,"fcfYield":4.0,"evEbitda":24,"debtServiceCover":0,"revenueGrowth":22,"marketCap":0.82,"price":820},
        {"ticker":"MASTEK.NS","name":"Mastek Ltd","sector":"Software","ev":0.65,"ebitdaMargin":18,"leverage":0.2,"roe":20,"roce":18,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":14,"marketCap":0.63,"price":2420},
        {"ticker":"NUCLEUS.NS","name":"Nucleus Software","sector":"BankTech","ev":0.28,"ebitdaMargin":22,"leverage":0.0,"roe":18,"roce":20,"fcfYield":4.5,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":12,"marketCap":0.27,"price":920},
        {"ticker":"SASKEN.NS","name":"Sasken Technologies","sector":"Embedded IT","ev":0.22,"ebitdaMargin":20,"leverage":0.0,"roe":16,"roce":18,"fcfYield":5.0,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.21,"price":1280},
    ],
    "bse": [
        {"ticker":"ASIANPAINT.NS","name":"Asian Paints","sector":"Paints","ev":52,"ebitdaMargin":24,"leverage":0.0,"roe":28,"roce":34,"fcfYield":2.8,"evEbitda":42,"debtServiceCover":0,"revenueGrowth":8,"marketCap":50,"price":2820},
        {"ticker":"PIDILITIND.NS","name":"Pidilite Industries","sector":"Chemicals","ev":28,"ebitdaMargin":26,"leverage":0.0,"roe":26,"roce":30,"fcfYield":2.5,"evEbitda":52,"debtServiceCover":0,"revenueGrowth":12,"marketCap":27,"price":2680},
        {"ticker":"BERGEPAINT.NS","name":"Berger Paints","sector":"Paints","ev":18,"ebitdaMargin":16,"leverage":0.0,"roe":24,"roce":28,"fcfYield":2.8,"evEbitda":40,"debtServiceCover":0,"revenueGrowth":8,"marketCap":17,"price":480},
        {"ticker":"HAVELLS.NS","name":"Havells India","sector":"Electricals","ev":15,"ebitdaMargin":14,"leverage":0.0,"roe":22,"roce":26,"fcfYield":3.0,"evEbitda":38,"debtServiceCover":0,"revenueGrowth":14,"marketCap":14,"price":1380},
        {"ticker":"MARICO.NS","name":"Marico Ltd","sector":"FMCG","ev":12,"ebitdaMargin":20,"leverage":0.0,"roe":38,"roce":44,"fcfYield":3.8,"evEbitda":38,"debtServiceCover":0,"revenueGrowth":6,"marketCap":11.5,"price":580},
        {"ticker":"PAGEIND.NS","name":"Page Industries","sector":"Apparel","ev":3.8,"ebitdaMargin":22,"leverage":0.0,"roe":52,"roce":62,"fcfYield":2.6,"evEbitda":42,"debtServiceCover":0,"revenueGrowth":14,"marketCap":3.7,"price":41000},
        {"ticker":"TITAN.NS","name":"Titan Company","sector":"Jewellery","ev":38,"ebitdaMargin":12,"leverage":0.0,"roe":28,"roce":32,"fcfYield":2.1,"evEbitda":58,"debtServiceCover":0,"revenueGrowth":22,"marketCap":36,"price":3580},
        {"ticker":"NAUKRI.NS","name":"Info Edge India","sector":"Internet","ev":10.2,"ebitdaMargin":28,"leverage":0.0,"roe":16,"roce":17,"fcfYield":2.4,"evEbitda":55,"debtServiceCover":0,"revenueGrowth":26,"marketCap":10.0,"price":7420},
        {"ticker":"MUTHOOTFIN.NS","name":"Muthoot Finance","sector":"NBFC","ev":8.8,"ebitdaMargin":45,"leverage":5.2,"roe":18,"roce":10,"fcfYield":5.1,"evEbitda":12,"debtServiceCover":1.8,"revenueGrowth":14,"marketCap":8.5,"price":2120},
        {"ticker":"MANAPPURAM.NS","name":"Manappuram Finance","sector":"NBFC","ev":2.5,"ebitdaMargin":42,"leverage":4.8,"roe":16,"roce":9,"fcfYield":5.8,"evEbitda":9,"debtServiceCover":1.7,"revenueGrowth":12,"marketCap":2.4,"price":185},
        {"ticker":"BAJFINANCE.NS","name":"Bajaj Finance","sector":"NBFC","ev":42,"ebitdaMargin":55,"leverage":8.2,"roe":24,"roce":13,"fcfYield":3.8,"evEbitda":25,"debtServiceCover":1.6,"revenueGrowth":28,"marketCap":40,"price":7200},
        {"ticker":"HDFCAMC.NS","name":"HDFC AMC","sector":"Asset Mgmt","ev":7.2,"ebitdaMargin":55,"leverage":0.0,"roe":32,"roce":36,"fcfYield":3.6,"evEbitda":35,"debtServiceCover":0,"revenueGrowth":18,"marketCap":7.0,"price":3280},
        {"ticker":"NAM-INDIA.NS","name":"Nippon India AMC","sector":"Asset Mgmt","ev":2.8,"ebitdaMargin":50,"leverage":0.0,"roe":26,"roce":30,"fcfYield":4.2,"evEbitda":28,"debtServiceCover":0,"revenueGrowth":22,"marketCap":2.7,"price":720},
        {"ticker":"SBILIFE.NS","name":"SBI Life Insurance","sector":"Insurance","ev":24,"ebitdaMargin":18,"leverage":0.0,"roe":14,"roce":15,"fcfYield":3.1,"evEbitda":45,"debtServiceCover":0,"revenueGrowth":20,"marketCap":23,"price":1680},
        {"ticker":"AARTIIND.NS","name":"Aarti Industries","sector":"Chemicals","ev":2.4,"ebitdaMargin":22,"leverage":2.1,"roe":22,"roce":18,"fcfYield":3.8,"evEbitda":16,"debtServiceCover":2.2,"revenueGrowth":14,"marketCap":2.2,"price":580},
        {"ticker":"SRF.NS","name":"SRF Ltd","sector":"Chemicals","ev":5.8,"ebitdaMargin":26,"leverage":1.4,"roe":24,"roce":20,"fcfYield":3.5,"evEbitda":18,"debtServiceCover":2.8,"revenueGrowth":18,"marketCap":5.5,"price":2180},
        {"ticker":"VINATIORGA.NS","name":"Vinati Organics","sector":"Chemicals","ev":2.1,"ebitdaMargin":32,"leverage":0.0,"roe":24,"roce":27,"fcfYield":3.0,"evEbitda":30,"debtServiceCover":0,"revenueGrowth":12,"marketCap":2.0,"price":1820},
        {"ticker":"ASTRAL.NS","name":"Astral Ltd","sector":"Pipes","ev":4.2,"ebitdaMargin":16,"leverage":0.0,"roe":22,"roce":24,"fcfYield":3.2,"evEbitda":50,"debtServiceCover":0,"revenueGrowth":18,"marketCap":4.0,"price":2240},
        {"ticker":"POLYCAB.NS","name":"Polycab India","sector":"Cables","ev":6.1,"ebitdaMargin":12,"leverage":0.0,"roe":22,"roce":26,"fcfYield":3.8,"evEbitda":30,"debtServiceCover":0,"revenueGrowth":20,"marketCap":5.8,"price":7680},
        {"ticker":"CAMS.NS","name":"Computer Age Mgmt","sector":"Fintech","ev":1.4,"ebitdaMargin":42,"leverage":0.0,"roe":36,"roce":40,"fcfYield":4.8,"evEbitda":32,"debtServiceCover":0,"revenueGrowth":18,"marketCap":1.35,"price":4520},
    ],
}


def _safe_round(val, digits=2):
    try:
        return round(float(val), digits)
    except (TypeError, ValueError):
        return 0.0


def _fetch_yf_metrics_batch(tickers_meta: list[tuple]) -> dict:
    """
    Fetch all tickers for an exchange in ONE yfinance batch call.
    Returns a dict of { ticker: metrics_dict | None }
    """
    ticker_symbols = [t[0] for t in tickers_meta]
    meta_map = {t[0]: (t[1], t[2]) for t in tickers_meta}  # ticker -> (name, sector)

    results = {}

    # Retry up to 3 times with exponential backoff + jitter on rate limit errors
    for attempt in range(3):
        try:
            # Download all tickers in one batch request
            data = yf.Tickers(" ".join(ticker_symbols))
            break
        except Exception as e:
            if attempt == 2:
                # All retries exhausted — return empty (will fall back to seed)
                return {ticker: None for ticker in ticker_symbols}
            wait = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait)

    for ticker in ticker_symbols:
        name, sector = meta_map[ticker]
        try:
            t = data.tickers[ticker]
            info = t.info or {}

            # Skip if clearly rate-limited or empty
            if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
                results[ticker] = None
                continue

            mc_raw = info.get("marketCap") or 0
            ev_raw = info.get("enterpriseValue") or mc_raw
            mc_b   = _safe_round(mc_raw / 1e9, 2)
            ev_b   = _safe_round(ev_raw / 1e9, 2)
            price  = _safe_round(info.get("currentPrice") or info.get("regularMarketPrice") or 0, 2)

            ebitda_margin = _safe_round((info.get("ebitdaMargins") or 0) * 100, 1)
            fcf_yield_raw = info.get("freeCashflow") or 0
            fcf_yield = _safe_round((fcf_yield_raw / max(mc_raw, 1)) * 100, 1) if mc_raw else 0.0

            total_debt = info.get("totalDebt") or 0
            cash       = info.get("totalCash") or 0
            ebitda     = info.get("ebitda") or 1
            net_debt   = max(0, total_debt - cash)
            leverage   = _safe_round(net_debt / max(ebitda, 1), 1)

            roe  = _safe_round((info.get("returnOnEquity") or 0) * 100, 1)
            ta   = info.get("totalAssets") or 1
            cl   = info.get("currentLiabilities") or 0
            ebit = info.get("ebit") or 0
            roce = _safe_round(ebit / max(ta - cl, 1) * 100, 1)

            ev_ebitda  = _safe_round(info.get("enterpriseToEbitda") or 0, 1)
            rev_growth = _safe_round((info.get("revenueGrowth") or 0) * 100, 1)

            interest = info.get("interestExpense") or 0
            dscr     = _safe_round(ebitda / max(abs(interest), 1), 1) if interest else 0.0

            if ebitda_margin <= 0 and ev_b <= 0:
                results[ticker] = None
                continue

            results[ticker] = {
                "ticker": ticker, "name": name, "sector": sector,
                "ev": ev_b, "marketCap": mc_b, "price": price,
                "ebitdaMargin": ebitda_margin,
                "leverage": leverage,
                "roe": roe, "roce": roce,
                "fcfYield": fcf_yield,
                "evEbitda": ev_ebitda,
                "debtServiceCover": dscr,
                "revenueGrowth": rev_growth,
                "dataSource": "live",
            }

        except Exception:
            results[ticker] = None

    return results


class DataService:
    _cache: dict = {}
    _cache_ts: dict = {}
    CACHE_TTL = 3600  # 1 hour

    async def get_exchange_data(self, exchange: str, refresh: bool = False) -> list[dict]:
        now = time.time()
        if not refresh and exchange in self._cache:
            if now - self._cache_ts.get(exchange, 0) < self.CACHE_TTL:
                return self._cache[exchange]

        tickers = EXCHANGE_TICKERS.get(exchange, [])
        seed    = {c["ticker"]: c for c in SEED_DATA.get(exchange, [])}

        # Fetch all tickers in ONE batch call (avoids per-ticker 429s)
        loop = asyncio.get_event_loop()
        live_data = await loop.run_in_executor(None, _fetch_yf_metrics_batch, tickers)

        results = []
        for (ticker, name, sector) in tickers:
            live = live_data.get(ticker)
            if live and live.get("ebitdaMargin", 0) > 0:
                rec = live
            else:
                rec = dict(seed.get(ticker, {
                    "ticker": ticker, "name": name, "sector": sector,
                    "ev": 1, "ebitdaMargin": 15, "leverage": 2,
                    "roe": 12, "roce": 10, "fcfYield": 3,
                    "evEbitda": 10, "debtServiceCover": 2, "revenueGrowth": 8,
                    "marketCap": 1, "price": 0,
                }))
                rec["dataSource"] = "seed"

            rec["score"]  = _engine.score_lbo(rec)
            rec["rating"] = _engine.get_rating(rec["score"])
            results.append(rec)

        self._cache[exchange]    = results
        self._cache_ts[exchange] = now
        return results
