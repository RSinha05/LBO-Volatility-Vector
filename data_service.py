"""
DataService — fetches real financial data via yfinance.
Falls back to curated seed data if the network / ticker is unavailable.
"""

import asyncio
import time
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
        {"ticker":"EPAM","name":"EPAM Systems","sector":"IT Services","ev":16.4,"ebitdaMargin":16,"leverage":0.0,"roe":21,"roce":19,"fcfYield":3.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":22,"marketCap":16,"price":280},
        {"ticker":"GLOB","name":"Globant SA","sector":"IT Services","ev":7.1,"ebitdaMargin":17,"leverage":0.1,"roe":17,"roce":15,"fcfYield":3.0,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":26,"marketCap":7,"price":160},
        {"ticker":"DXC","name":"DXC Technology","sector":"IT Services","ev":8.2,"ebitdaMargin":16,"leverage":2.6,"roe":11,"roce":9,"fcfYield":5.8,"evEbitda":7,"debtServiceCover":2.4,"revenueGrowth":-3,"marketCap":6,"price":22},
        {"ticker":"CDAY","name":"Ceridian HCM","sector":"HRTech","ev":14,"ebitdaMargin":24,"leverage":3.3,"roe":8,"roce":7,"fcfYield":2.9,"evEbitda":13,"debtServiceCover":1.8,"revenueGrowth":18,"marketCap":12,"price":55},
        {"ticker":"EVOP","name":"EVO Payments","sector":"Payments","ev":2.1,"ebitdaMargin":28,"leverage":3.0,"roe":15,"roce":12,"fcfYield":4.8,"evEbitda":9,"debtServiceCover":2.6,"revenueGrowth":14,"marketCap":1.8,"price":18},
        {"ticker":"DAVA","name":"Endava PLC","sector":"IT Services","ev":2.9,"ebitdaMargin":18,"leverage":0.0,"roe":19,"roce":17,"fcfYield":4.1,"evEbitda":11,"debtServiceCover":0,"revenueGrowth":29,"marketCap":2.8,"price":42},
        {"ticker":"ALRM","name":"Alarm.com Holdings","sector":"IoT Software","ev":2.5,"ebitdaMargin":16,"leverage":0.0,"roe":12,"roce":11,"fcfYield":3.7,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":16,"marketCap":2.4,"price":45},
        {"ticker":"RAMP","name":"LiveRamp Holdings","sector":"Data Tech","ev":2.2,"ebitdaMargin":10,"leverage":0.0,"roe":5,"roce":5,"fcfYield":2.1,"evEbitda":16,"debtServiceCover":0,"revenueGrowth":14,"marketCap":2.2,"price":32},
        {"ticker":"NTWK","name":"NetSol Technologies","sector":"Software","ev":0.14,"ebitdaMargin":20,"leverage":0.3,"roe":13,"roce":11,"fcfYield":3.9,"evEbitda":7,"debtServiceCover":0,"revenueGrowth":11,"marketCap":0.13,"price":6},
        {"ticker":"PPTC","name":"Park Place Technologies","sector":"IT Services","ev":1.2,"ebitdaMargin":45,"leverage":2.4,"roe":32,"roce":26,"fcfYield":7.2,"evEbitda":6,"debtServiceCover":3.2,"revenueGrowth":12,"marketCap":1.1,"price":38},
        {"ticker":"NTCT","name":"NetScout Systems","sector":"Cybersecurity","ev":1.1,"ebitdaMargin":32,"leverage":0.6,"roe":14,"roce":13,"fcfYield":5.9,"evEbitda":8,"debtServiceCover":3.5,"revenueGrowth":8,"marketCap":1.05,"price":18},
        {"ticker":"CLFD","name":"Clearfield Inc","sector":"Telecom Infra","ev":0.38,"ebitdaMargin":26,"leverage":0.0,"roe":22,"roce":24,"fcfYield":6.2,"evEbitda":8,"debtServiceCover":0,"revenueGrowth":18,"marketCap":0.37,"price":28},
        {"ticker":"MPLX","name":"MicroStrategy Analytics","sector":"Analytics","ev":0.85,"ebitdaMargin":38,"leverage":1.8,"roe":28,"roce":24,"fcfYield":6.8,"evEbitda":7,"debtServiceCover":3.0,"revenueGrowth":16,"marketCap":0.80,"price":24},
    ],
    "dow": [
        {"ticker":"DHR","name":"Danaher Corp","sector":"Healthcare","ev":88,"ebitdaMargin":32,"leverage":1.4,"roe":14,"roce":12,"fcfYield":4.2,"evEbitda":13,"debtServiceCover":3.5,"revenueGrowth":4,"marketCap":80,"price":220},
        {"ticker":"ROP","name":"Roper Technologies","sector":"Industrials","ev":54,"ebitdaMargin":38,"leverage":2.1,"roe":12,"roce":10,"fcfYield":3.8,"evEbitda":14,"debtServiceCover":2.8,"revenueGrowth":7,"marketCap":52,"price":560},
        {"ticker":"CTAS","name":"Cintas Corp","sector":"Business Svcs","ev":62,"ebitdaMargin":26,"leverage":1.9,"roe":37,"roce":22,"fcfYield":2.8,"evEbitda":11,"debtServiceCover":3.1,"revenueGrowth":9,"marketCap":58,"price":170},
        {"ticker":"WCN","name":"Waste Connections","sector":"Industrials","ev":42,"ebitdaMargin":31,"leverage":2.8,"roe":16,"roce":13,"fcfYield":3.3,"evEbitda":10,"debtServiceCover":2.4,"revenueGrowth":8,"marketCap":38,"price":170},
        {"ticker":"TDG","name":"TransDigm Group","sector":"Aerospace","ev":55,"ebitdaMargin":44,"leverage":7.1,"roe":80,"roce":15,"fcfYield":4.6,"evEbitda":10,"debtServiceCover":1.7,"revenueGrowth":11,"marketCap":42,"price":1200},
        {"ticker":"ROL","name":"Rollins Inc","sector":"Consumer Svcs","ev":18,"ebitdaMargin":24,"leverage":1.1,"roe":35,"roce":28,"fcfYield":3.5,"evEbitda":9,"debtServiceCover":4.0,"revenueGrowth":9,"marketCap":17,"price":46},
        {"ticker":"IQV","name":"IQVIA Holdings","sector":"Healthcare","ev":42,"ebitdaMargin":22,"leverage":3.1,"roe":15,"roce":11,"fcfYield":4.4,"evEbitda":13,"debtServiceCover":2.2,"revenueGrowth":6,"marketCap":38,"price":200},
        {"ticker":"SPB","name":"Spectrum Brands","sector":"Consumer","ev":5.8,"ebitdaMargin":17,"leverage":4.1,"roe":15,"roce":11,"fcfYield":4.2,"evEbitda":12,"debtServiceCover":1.8,"revenueGrowth":3,"marketCap":4.5,"price":82},
        {"ticker":"HURN","name":"Huron Consulting","sector":"Consulting","ev":1.4,"ebitdaMargin":20,"leverage":2.2,"roe":17,"roce":14,"fcfYield":4.5,"evEbitda":9,"debtServiceCover":2.5,"revenueGrowth":8,"marketCap":1.3,"price":110},
        {"ticker":"AMED","name":"Amedisys Inc","sector":"Healthcare","ev":3.2,"ebitdaMargin":18,"leverage":1.8,"roe":19,"roce":16,"fcfYield":4.9,"evEbitda":7,"debtServiceCover":3.1,"revenueGrowth":11,"marketCap":3.0,"price":96},
        {"ticker":"HSC","name":"Harsco Corp","sector":"Industrials","ev":1.8,"ebitdaMargin":14,"leverage":3.6,"roe":12,"roce":9,"fcfYield":4.8,"evEbitda":8,"debtServiceCover":1.8,"revenueGrowth":4,"marketCap":1.2,"price":11},
        {"ticker":"GTLS","name":"Chart Industries","sector":"Industrials","ev":4.6,"ebitdaMargin":16,"leverage":4.0,"roe":10,"roce":8,"fcfYield":3.9,"evEbitda":9,"debtServiceCover":1.7,"revenueGrowth":12,"marketCap":3.8,"price":120},
        {"ticker":"ENOV","name":"Enovis Corp","sector":"MedTech","ev":3.8,"ebitdaMargin":18,"leverage":3.5,"roe":6,"roce":5,"fcfYield":3.5,"evEbitda":10,"debtServiceCover":1.9,"revenueGrowth":14,"marketCap":3.2,"price":42},
        {"ticker":"PERI","name":"Perion Network","sector":"AdTech","ev":0.7,"ebitdaMargin":24,"leverage":0.0,"roe":18,"roce":16,"fcfYield":6.1,"evEbitda":7,"debtServiceCover":0,"revenueGrowth":22,"marketCap":0.68,"price":14},
        {"ticker":"SABR","name":"Sabre Corp","sector":"Travel Tech","ev":5.9,"ebitdaMargin":18,"leverage":9.2,"roe":-15,"roce":4,"fcfYield":2.8,"evEbitda":12,"debtServiceCover":0.9,"revenueGrowth":18,"marketCap":2.8,"price":3.2},
        {"ticker":"VVI","name":"Viad Corp","sector":"Travel Svcs","ev":1.2,"ebitdaMargin":16,"leverage":3.1,"roe":10,"roce":8,"fcfYield":4.6,"evEbitda":9,"debtServiceCover":1.9,"revenueGrowth":10,"marketCap":0.9,"price":38},
        {"ticker":"GTX","name":"Garrett Motion","sector":"Automotive","ev":4.1,"ebitdaMargin":22,"leverage":4.8,"roe":60,"roce":18,"fcfYield":8.1,"evEbitda":6,"debtServiceCover":1.9,"revenueGrowth":7,"marketCap":3.5,"price":10},
        {"ticker":"MPLN","name":"MultiPlan Corp","sector":"Healthcare IT","ev":5.8,"ebitdaMargin":52,"leverage":5.5,"roe":18,"roce":14,"fcfYield":6.8,"evEbitda":7,"debtServiceCover":1.6,"revenueGrowth":2,"marketCap":2.8,"price":4.5},
        {"ticker":"SPR","name":"Spirit AeroSystems","sector":"Aerospace","ev":4.2,"ebitdaMargin":8,"leverage":8.5,"roe":-20,"roce":3,"fcfYield":2.2,"evEbitda":14,"debtServiceCover":0.8,"revenueGrowth":12,"marketCap":2.8,"price":22},
        {"ticker":"NCI","name":"NCI Info Systems","sector":"Defense IT","ev":6,"ebitdaMargin":22,"leverage":3.2,"roe":22,"roce":18,"fcfYield":5.6,"evEbitda":8,"debtServiceCover":2.7,"revenueGrowth":10,"marketCap":5.5,"price":42},
        {"ticker":"CVGX","name":"ConvergEx Group","sector":"Fintech","ev":2.1,"ebitdaMargin":44,"leverage":2.5,"roe":28,"roce":24,"fcfYield":7.2,"evEbitda":6,"debtServiceCover":3.4,"revenueGrowth":9,"marketCap":1.9,"price":22},
        {"ticker":"BNDL","name":"Brand Industrial Svcs","sector":"Industrials","ev":3.8,"ebitdaMargin":22,"leverage":3.4,"roe":26,"roce":21,"fcfYield":5.8,"evEbitda":7,"debtServiceCover":2.6,"revenueGrowth":8,"marketCap":3.4,"price":38},
    ],
    "nse": [
        {"ticker":"INFY.NS","name":"Infosys Ltd","sector":"IT Services","ev":78,"ebitdaMargin":24,"leverage":0.0,"roe":31,"roce":40,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":7,"marketCap":75,"price":1520},
        {"ticker":"TCS.NS","name":"TCS Ltd","sector":"IT Services","ev":165,"ebitdaMargin":27,"leverage":0.0,"roe":48,"roce":57,"fcfYield":3.8,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":7,"marketCap":162,"price":3800},
        {"ticker":"WIPRO.NS","name":"Wipro Ltd","sector":"IT Services","ev":32,"ebitdaMargin":18,"leverage":0.1,"roe":17,"roce":19,"fcfYield":5.1,"evEbitda":16,"debtServiceCover":0,"revenueGrowth":5,"marketCap":31,"price":455},
        {"ticker":"HCLTECH.NS","name":"HCL Technologies","sector":"IT Services","ev":45,"ebitdaMargin":22,"leverage":0.0,"roe":25,"roce":29,"fcfYield":4.6,"evEbitda":17,"debtServiceCover":0,"revenueGrowth":9,"marketCap":44,"price":1580},
        {"ticker":"MPHASIS.NS","name":"Mphasis Ltd","sector":"IT Services","ev":5.4,"ebitdaMargin":20,"leverage":0.2,"roe":24,"roce":26,"fcfYield":4.9,"evEbitda":20,"debtServiceCover":0,"revenueGrowth":11,"marketCap":5.2,"price":2740},
        {"ticker":"PERSISTENT.NS","name":"Persistent Systems","sector":"Software","ev":5.1,"ebitdaMargin":18,"leverage":0.0,"roe":28,"roce":32,"fcfYield":3.6,"evEbitda":22,"debtServiceCover":0,"revenueGrowth":22,"marketCap":5.0,"price":5200},
        {"ticker":"COFORGE.NS","name":"Coforge Ltd","sector":"IT Services","ev":2.8,"ebitdaMargin":17,"leverage":1.1,"roe":27,"roce":29,"fcfYield":4.2,"evEbitda":18,"debtServiceCover":0,"revenueGrowth":18,"marketCap":2.7,"price":8500},
        {"ticker":"KPITTECH.NS","name":"KPIT Technologies","sector":"Auto IT","ev":2.2,"ebitdaMargin":20,"leverage":0.0,"roe":29,"roce":33,"fcfYield":3.4,"evEbitda":24,"debtServiceCover":0,"revenueGrowth":35,"marketCap":2.1,"price":1820},
        {"ticker":"TATAELXSI.NS","name":"Tata Elxsi","sector":"Design IT","ev":2.9,"ebitdaMargin":30,"leverage":0.0,"roe":39,"roce":45,"fcfYield":3.1,"evEbitda":28,"debtServiceCover":0,"revenueGrowth":28,"marketCap":2.8,"price":7200},
        {"ticker":"ZENSARTECH.NS","name":"Zensar Technologies","sector":"IT Services","ev":1.0,"ebitdaMargin":14,"leverage":0.0,"roe":18,"roce":20,"fcfYield":5.8,"evEbitda":11,"debtServiceCover":0,"revenueGrowth":14,"marketCap":0.98,"price":720},
        {"ticker":"CYIENT.NS","name":"Cyient Ltd","sector":"Engg IT","ev":0.9,"ebitdaMargin":15,"leverage":0.2,"roe":17,"roce":19,"fcfYield":4.7,"evEbitda":13,"debtServiceCover":0,"revenueGrowth":12,"marketCap":0.88,"price":1450},
        {"ticker":"TANLA.NS","name":"Tanla Platforms","sector":"CPaaS","ev":0.9,"ebitdaMargin":22,"leverage":0.0,"roe":28,"roce":31,"fcfYield":5.6,"evEbitda":11,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.88,"price":780},
        {"ticker":"ROUTE.NS","name":"Route Mobile","sector":"CPaaS","ev":0.55,"ebitdaMargin":14,"leverage":0.5,"roe":16,"roce":18,"fcfYield":4.2,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":22,"marketCap":0.53,"price":1580},
        {"ticker":"INTELLECT.NS","name":"Intellect Design","sector":"BankTech","ev":0.7,"ebitdaMargin":16,"leverage":0.2,"roe":15,"roce":17,"fcfYield":3.9,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":20,"marketCap":0.68,"price":860},
        {"ticker":"NEWGEN.NS","name":"Newgen Software","sector":"Software","ev":0.5,"ebitdaMargin":19,"leverage":0.0,"roe":21,"roce":23,"fcfYield":4.8,"evEbitda":12,"debtServiceCover":0,"revenueGrowth":24,"marketCap":0.48,"price":1080},
        {"ticker":"BSOFT.NS","name":"Birlasoft Ltd","sector":"IT Services","ev":0.7,"ebitdaMargin":14,"leverage":0.0,"roe":17,"roce":19,"fcfYield":4.6,"evEbitda":13,"debtServiceCover":0,"revenueGrowth":16,"marketCap":0.68,"price":520},
        {"ticker":"HAPPSTMNDS.NS","name":"Happiest Minds","sector":"Digital IT","ev":0.7,"ebitdaMargin":22,"leverage":0.0,"roe":30,"roce":34,"fcfYield":4.0,"evEbitda":14,"debtServiceCover":0,"revenueGrowth":26,"marketCap":0.68,"price":820},
        {"ticker":"MASTEK.NS","name":"Mastek Ltd","sector":"Software","ev":0.55,"ebitdaMargin":16,"leverage":0.4,"roe":22,"roce":24,"fcfYield":5.2,"evEbitda":13,"debtServiceCover":0,"revenueGrowth":20,"marketCap":0.53,"price":2340},
        {"ticker":"NUCLEUS.NS","name":"Nucleus Software","sector":"BankTech","ev":0.2,"ebitdaMargin":24,"leverage":0.0,"roe":19,"roce":22,"fcfYield":5.8,"evEbitda":9,"debtServiceCover":0,"revenueGrowth":12,"marketCap":0.19,"price":1580},
        {"ticker":"SASKEN.NS","name":"Sasken Technologies","sector":"Embedded IT","ev":0.18,"ebitdaMargin":17,"leverage":0.0,"roe":16,"roce":18,"fcfYield":5.3,"evEbitda":10,"debtServiceCover":0,"revenueGrowth":8,"marketCap":0.17,"price":1920},
    ],
    "bse": [
        {"ticker":"ASIANPAINT.NS","name":"Asian Paints","sector":"Paints","ev":42,"ebitdaMargin":22,"leverage":0.0,"roe":28,"roce":35,"fcfYield":3.2,"evEbitda":42,"debtServiceCover":0,"revenueGrowth":8,"marketCap":40,"price":2820},
        {"ticker":"PIDILITIND.NS","name":"Pidilite Industries","sector":"Chemicals","ev":18,"ebitdaMargin":22,"leverage":0.0,"roe":28,"roce":32,"fcfYield":2.8,"evEbitda":55,"debtServiceCover":0,"revenueGrowth":12,"marketCap":17,"price":3420},
        {"ticker":"BERGEPAINT.NS","name":"Berger Paints","sector":"Paints","ev":8.2,"ebitdaMargin":18,"leverage":0.0,"roe":25,"roce":30,"fcfYield":2.9,"evEbitda":38,"debtServiceCover":0,"revenueGrowth":7,"marketCap":8.0,"price":560},
        {"ticker":"HAVELLS.NS","name":"Havells India","sector":"Electricals","ev":8.5,"ebitdaMargin":14,"leverage":0.0,"roe":22,"roce":26,"fcfYield":3.4,"evEbitda":40,"debtServiceCover":0,"revenueGrowth":14,"marketCap":8.2,"price":1580},
        {"ticker":"MARICO.NS","name":"Marico Ltd","sector":"FMCG","ev":8.1,"ebitdaMargin":20,"leverage":0.0,"roe":38,"roce":47,"fcfYield":4.0,"evEbitda":35,"debtServiceCover":0,"revenueGrowth":5,"marketCap":7.8,"price":605},
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


def _fetch_yf_metrics(ticker: str, name: str, sector: str) -> dict | None:
    """Pull key financials from yfinance. Returns None on any failure."""
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        # market cap / EV
        mc_raw = info.get("marketCap") or 0
        ev_raw = info.get("enterpriseValue") or mc_raw
        mc_b   = _safe_round(mc_raw / 1e9, 2)
        ev_b   = _safe_round(ev_raw / 1e9, 2)
        price  = _safe_round(info.get("currentPrice") or info.get("regularMarketPrice") or 0, 2)

        # margins / yields
        ebitda_margin = _safe_round((info.get("ebitdaMargins") or 0) * 100, 1)
        fcf_yield_raw = info.get("freeCashflow") or 0
        fcf_yield = _safe_round((fcf_yield_raw / max(mc_raw, 1)) * 100, 1) if mc_raw else 0.0

        # leverage
        total_debt = info.get("totalDebt") or 0
        cash       = info.get("totalCash") or 0
        ebitda     = info.get("ebitda") or 1
        net_debt   = max(0, total_debt - cash)
        leverage   = _safe_round(net_debt / max(ebitda, 1), 1)

        # returns
        roe  = _safe_round((info.get("returnOnEquity") or 0) * 100, 1)
        # ROCE approximation: EBIT / (Total Assets – Current Liabilities)
        ta   = info.get("totalAssets") or 1
        cl   = info.get("currentLiabilities") or 0
        ebit = info.get("ebit") or 0
        roce = _safe_round(ebit / max(ta - cl, 1) * 100, 1)

        ev_ebitda = _safe_round(info.get("enterpriseToEbitda") or 0, 1)
        rev_growth = _safe_round((info.get("revenueGrowth") or 0) * 100, 1)

        # DSCR proxy: EBITDA / (interest + current debt maturities)
        interest   = info.get("interestExpense") or 0
        dscr       = _safe_round(ebitda / max(abs(interest), 1), 1) if interest else 0.0

        if ebitda_margin <= 0 and ev_b <= 0:
            return None  # useless record

        return {
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
        return None


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

        results = []
        loop = asyncio.get_event_loop()

        for (ticker, name, sector) in tickers:
            live = await loop.run_in_executor(None, _fetch_yf_metrics, ticker, name, sector)
            if live and live.get("ebitdaMargin", 0) > 0:
                rec = live
            else:
                # use seed fallback
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
