"""
BOLA.AI Backend — FBref Scraper + API
======================================
Stack  : Python 3.10+, FastAPI, BeautifulSoup4, pandas
Deploy : Railway / Render / VPS (lihat README.md)

Endpoints:
  GET /team/{team_slug}          → stats lengkap tim dari FBref
  GET /match/{home}/{away}       → head-to-head + stats gabungan
  GET /squad/{team_slug}         → daftar pemain + cedera estimasi
  GET /standings/{league}        → klasemen liga
  GET /health                    → cek server hidup
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
from bs4 import BeautifulSoup
import pandas as pd
import json, re, time, asyncio
from datetime import datetime, timedelta
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bolaai")

app = FastAPI(
    title="BOLA.AI Backend",
    description="FBref scraper API untuk prediksi sepak bola",
    version="1.0.0"
)

# ─── CORS — izinkan request dari claude.ai dan localhost ─────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # ganti ke domain spesifik di production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── IN-MEMORY CACHE (TTL 6 jam) ─────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL = 60 * 60 * 6  # 6 jam dalam detik

def cache_get(key: str):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del _cache[key]
    return None

def cache_set(key: str, data):
    _cache[key] = (data, time.time())

# ─── FBREF SLUGS — mapping nama tim ke URL slug FBref ────────────────────────
TEAM_SLUGS = {
    # Bundesliga
    "mainz":       ("d7a486cd", "1-FSV-Mainz-05"),
    "stuttgart":   ("598bc722", "VfB-Stuttgart"),
    "bayern":      ("054efa67", "Bayern-Munich"),
    "dortmund":    ("add600ae", "Borussia-Dortmund"),
    "leverkusen":  ("c7a9f859", "Bayer-Leverkusen"),
    "leipzig":     ("acbb6b5b", "RB-Leipzig"),
    "frankfurt":   ("f0ac8ee6", "Eintracht-Frankfurt"),
    "freiburg":    ("a486e511", "SC-Freiburg"),
    "hoffenheim":  ("033ea6b8", "Hoffenheim"),
    "wolfsburg":   ("4eaa11d7", "Wolfsburg"),
    # Premier League
    "manchester-city":   ("b8fd03ef", "Manchester-City"),
    "arsenal":           ("18bb7c10", "Arsenal"),
    "liverpool":         ("822bd0ba", "Liverpool"),
    "chelsea":           ("cff3d9bb", "Chelsea"),
    "tottenham":         ("361ca564", "Tottenham-Hotspur"),
    "manchester-united": ("19538871", "Manchester-United"),
    "newcastle":         ("b2b47a98", "Newcastle-United"),
    # La Liga
    "real-madrid":   ("53a2f082", "Real-Madrid"),
    "barcelona":     ("206d90db", "Barcelona"),
    "atletico":      ("db3b9613", "Atletico-Madrid"),
    # Serie A
    "inter":         ("d609edc0", "Inter-Milan"),
    "milan":         ("dc56fe14", "AC-Milan"),
    "juventus":      ("e0652b02", "Juventus"),
    "napoli":        ("d48ad4ff", "Napoli"),
    # Ligue 1
    "psg":           ("e2d8892c", "Paris-Saint-Germain"),
}

LEAGUE_SLUGS = {
    "bundesliga": "20",
    "premier-league": "9",
    "la-liga": "12",
    "serie-a": "11",
    "ligue-1": "13",
    "champions-league": "8",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://fbref.com/",
}

# ─── FBREF SCRAPER FUNCTIONS ─────────────────────────────────────────────────

async def fetch_page(url: str) -> BeautifulSoup:
    """Fetch halaman HTML dari FBref dengan rate limiting."""
    await asyncio.sleep(4)  # FBref rate limit: max 1 req/3 detik
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        resp = await client.get(url, headers=HEADERS)
        resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")

def parse_float(val: str) -> Optional[float]:
    """Parse angka dari string FBref, handle edge cases."""
    if not val or val in ["-", "", "N/A", None]:
        return None
    try:
        return float(val.strip().replace(",", ""))
    except ValueError:
        return None

async def scrape_team_stats(team_slug: str) -> dict:
    """Scrape statistik tim dari halaman squad FBref."""
    cached = cache_get(f"team_{team_slug}")
    if cached:
        logger.info(f"Cache hit: team_{team_slug}")
        return cached

    if team_slug not in TEAM_SLUGS:
        raise HTTPException(404, f"Team '{team_slug}' tidak ditemukan. Tersedia: {list(TEAM_SLUGS.keys())}")

    team_id, team_name = TEAM_SLUGS[team_slug]
    url = f"https://fbref.com/en/squads/{team_id}/2024-2025/{team_name}-Stats"
    logger.info(f"Scraping: {url}")

    soup = await fetch_page(url)
    result = {"team": team_name, "slug": team_slug, "source": url, "scraped_at": datetime.now().isoformat()}

    # ── 1. SHOOTING STATS (xG, Goals, Shots) ──────────────────────────────
    shooting = {}
    tbl = soup.find("table", {"id": "stats_shooting"}) or soup.find("table", {"id": "stats_squads_shooting_for"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            # Cari baris total tim (biasanya baris terakhir atau "Squad Total")
            last = df.iloc[-1]
            shooting = {
                "goals":       parse_float(str(last.get("Gls", ""))),
                "shots":       parse_float(str(last.get("Sh", ""))),
                "shots_on_target": parse_float(str(last.get("SoT", ""))),
                "xg":          parse_float(str(last.get("xG", ""))),
                "xg_per_90":   parse_float(str(last.get("xG/90", last.get("xG.1", "")))),
                "npxg":        parse_float(str(last.get("npxG", ""))),
                "goals_per_shot": parse_float(str(last.get("G/Sh", ""))),
            }
        except Exception as e:
            logger.warning(f"Shooting parse error: {e}")

    # ── 2. PASSING STATS ───────────────────────────────────────────────────
    passing = {}
    tbl = soup.find("table", {"id": "stats_passing"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            last = df.iloc[-1]
            passing = {
                "passes_completed": parse_float(str(last.get("Cmp", ""))),
                "passes_attempted": parse_float(str(last.get("Att", ""))),
                "pass_accuracy":    parse_float(str(last.get("Cmp%", ""))),
                "key_passes":       parse_float(str(last.get("KP", ""))),
                "progressive_passes": parse_float(str(last.get("PrgP", ""))),
                "xag":             parse_float(str(last.get("xAG", ""))),
            }
        except Exception as e:
            logger.warning(f"Passing parse error: {e}")

    # ── 3. DEFENSIVE STATS ─────────────────────────────────────────────────
    defense = {}
    tbl = soup.find("table", {"id": "stats_defense"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            last = df.iloc[-1]
            defense = {
                "tackles":         parse_float(str(last.get("Tkl", ""))),
                "tackle_success":  parse_float(str(last.get("TklW", ""))),
                "pressures":       parse_float(str(last.get("Press", ""))),
                "pressure_success":parse_float(str(last.get("Succ%", ""))),
                "blocks":          parse_float(str(last.get("Blocks", ""))),
                "interceptions":   parse_float(str(last.get("Int", ""))),
                "clearances":      parse_float(str(last.get("Clr", ""))),
            }
        except Exception as e:
            logger.warning(f"Defense parse error: {e}")

    # ── 4. POSSESSION / MISC ───────────────────────────────────────────────
    possession = {}
    tbl = soup.find("table", {"id": "stats_possession"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            last = df.iloc[-1]
            possession = {
                "possession_pct":      parse_float(str(last.get("Poss", ""))),
                "progressive_carries": parse_float(str(last.get("PrgC", ""))),
                "touches_att_pen":     parse_float(str(last.get("AttPen", ""))),
                "carries":             parse_float(str(last.get("Carries", ""))),
            }
        except Exception as e:
            logger.warning(f"Possession parse error: {e}")

    # ── 5. GOALKEEPER STATS ────────────────────────────────────────────────
    gk = {}
    tbl = soup.find("table", {"id": "stats_keeper"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            # Ambil GK utama (menit terbanyak)
            if "Min" in df.columns:
                df["Min"] = pd.to_numeric(df["Min"].astype(str).str.replace(",",""), errors="coerce")
                row = df.loc[df["Min"].idxmax()]
            else:
                row = df.iloc[0]
            gk = {
                "goalkeeper":    str(row.get("Player", "Unknown")),
                "ga90":         parse_float(str(row.get("GA90", ""))),
                "save_pct":     parse_float(str(row.get("Save%", ""))),
                "cs_pct":       parse_float(str(row.get("CS%", ""))),
                "psxg":         parse_float(str(row.get("PSxG", ""))),
                "psxg_vs_ga":   parse_float(str(row.get("PSxG-GA", row.get("+/-", "")))),
            }
        except Exception as e:
            logger.warning(f"GK parse error: {e}")

    # ── 6. SQUAD LIST (pemain aktif) ───────────────────────────────────────
    players = []
    tbl = soup.find("table", {"id": "stats_standard"})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]
            # Filter baris pemain valid (ada nama & posisi)
            for _, row in df.iterrows():
                name = str(row.get("Player", ""))
                pos  = str(row.get("Pos", ""))
                if not name or name in ["Player", "nan"] or not pos or pos == "nan":
                    continue
                players.append({
                    "name":    name,
                    "pos":     pos,
                    "age":     str(row.get("Age", "")),
                    "nation":  str(row.get("Nation", "")),
                    "goals":   parse_float(str(row.get("Gls", ""))),
                    "assists": parse_float(str(row.get("Ast", ""))),
                    "xg":      parse_float(str(row.get("xG", ""))),
                    "minutes": parse_float(str(row.get("Min", ""))),
                })
        except Exception as e:
            logger.warning(f"Squad parse error: {e}")

    result.update({
        "shooting":  shooting,
        "passing":   passing,
        "defense":   defense,
        "possession": possession,
        "goalkeeper": gk,
        "squad":     players[:25],  # top 25 pemain
    })

    cache_set(f"team_{team_slug}", result)
    return result


async def scrape_fixtures_and_form(team_slug: str) -> dict:
    """Scrape hasil pertandingan terakhir tim (form 5 laga)."""
    cached = cache_get(f"form_{team_slug}")
    if cached:
        return cached

    if team_slug not in TEAM_SLUGS:
        raise HTTPException(404, f"Team '{team_slug}' tidak ditemukan")

    team_id, team_name = TEAM_SLUGS[team_slug]
    url = f"https://fbref.com/en/squads/{team_id}/2024-2025/matchlogs/all_comps/schedule/{team_name}-Match-Logs-All-Competitions"

    soup = await fetch_page(url)
    matches = []

    tbl = soup.find("table", {"id": re.compile(r"matchlogs")})
    if tbl:
        try:
            df = pd.read_html(str(tbl))[0]
            df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]

            # Filter hanya laga yang sudah selesai
            played = df[df["Result"].isin(["W", "D", "L"])].copy()
            played = played.sort_values("Date", ascending=False).head(10)

            for _, row in played.iterrows():
                gf = parse_float(str(row.get("GF", "")))
                ga = parse_float(str(row.get("GA", "")))
                xg_for  = parse_float(str(row.get("xG", "")))
                xg_ag   = parse_float(str(row.get("xGA", "")))
                matches.append({
                    "date":       str(row.get("Date", "")),
                    "competition":str(row.get("Comp", "")),
                    "venue":      str(row.get("Venue", "")),    # Home/Away
                    "opponent":   str(row.get("Opponent", "")),
                    "result":     str(row.get("Result", "")),
                    "gf":         gf,
                    "ga":         ga,
                    "xg_for":    xg_for,
                    "xg_against": xg_ag,
                    "possession": parse_float(str(row.get("Poss", ""))),
                })
        except Exception as e:
            logger.warning(f"Fixtures parse error: {e}")

    # Kalkulasi form metrics
    last5 = matches[:5]
    form_str = "".join([m["result"] for m in last5])
    pts_last5 = sum(3 if m["result"]=="W" else 1 if m["result"]=="D" else 0 for m in last5)
    avg_xg_for = sum(m["xg_for"] or 0 for m in last5) / max(len(last5),1)
    avg_xg_ag  = sum(m["xg_against"] or 0 for m in last5) / max(len(last5),1)

    data = {
        "team": team_name,
        "form_string": form_str,
        "pts_last5":   pts_last5,
        "avg_xg_for_last5":     round(avg_xg_for, 2),
        "avg_xg_against_last5": round(avg_xg_ag, 2),
        "last_10_matches": matches,
    }
    cache_set(f"form_{team_slug}", data)
    return data


async def scrape_h2h(home_slug: str, away_slug: str) -> dict:
    """
    H2H diambil dari halaman fixture/head-to-head FBref.
    FBref tidak punya endpoint H2H langsung, jadi kita filter
    dari match log home team vs away team.
    """
    cached = cache_get(f"h2h_{home_slug}_{away_slug}")
    if cached:
        return cached

    home_form = await scrape_fixtures_and_form(home_slug)
    home_name = TEAM_SLUGS[away_slug][1].replace("-", " ")

    h2h_matches = [
        m for m in home_form["last_10_matches"]
        if home_name.lower() in m["opponent"].lower()
    ]

    home_w = sum(1 for m in h2h_matches if m["result"]=="W")
    draws  = sum(1 for m in h2h_matches if m["result"]=="D")
    home_l = sum(1 for m in h2h_matches if m["result"]=="L")

    data = {
        "home_team": TEAM_SLUGS[home_slug][1],
        "away_team": TEAM_SLUGS[away_slug][1],
        "from_last_10_home_logs": True,
        "home_wins": home_w,
        "draws": draws,
        "away_wins": home_l,
        "matches": h2h_matches,
        "note": "H2H dari match log home team 10 laga terakhir. Untuk H2H lebih lengkap gunakan API-Football."
    }
    cache_set(f"h2h_{home_slug}_{away_slug}", data)
    return data


# ─── API ENDPOINTS ────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat(), "cache_keys": len(_cache)}


@app.get("/teams")
async def list_teams():
    """Daftar semua tim yang tersedia."""
    return {"teams": list(TEAM_SLUGS.keys()), "total": len(TEAM_SLUGS)}


@app.get("/team/{team_slug}")
async def get_team_stats(team_slug: str):
    """
    Statistik lengkap tim dari FBref.
    Includes: shooting (xG), passing, defense, possession, GK, squad list.
    Cache: 6 jam
    """
    try:
        data = await scrape_team_stats(team_slug)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scraping {team_slug}: {e}")
        raise HTTPException(500, f"Gagal scraping data: {str(e)}")


@app.get("/form/{team_slug}")
async def get_team_form(team_slug: str):
    """
    Form 5-10 laga terakhir tim (semua kompetisi).
    Includes: hasil, xG for/against per laga, possession.
    """
    try:
        data = await scrape_fixtures_and_form(team_slug)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Gagal scraping form: {str(e)}")


@app.get("/match/{home_slug}/{away_slug}")
async def get_match_analysis(home_slug: str, away_slug: str):
    """
    Analisis lengkap matchup: stats home + away + H2H + derived metrics.
    Ini endpoint utama yang dipanggil BOLA.AI frontend.
    """
    try:
        # Fetch parallel dengan delay agar tidak kena rate limit FBref
        home_stats = await scrape_team_stats(home_slug)
        away_stats = await scrape_team_stats(away_slug)
        home_form  = await scrape_fixtures_and_form(home_slug)
        away_form  = await scrape_fixtures_and_form(away_slug)
        h2h        = await scrape_h2h(home_slug, away_slug)

        # ── Derived metrics untuk model prediksi ──────────────────────────
        h_xg  = home_stats["shooting"].get("xg") or 0
        a_xg  = away_stats["shooting"].get("xg") or 0
        h_matches = max(1, len([m for m in home_form["last_10_matches"] if m["result"] in ["W","D","L"]]))
        a_matches = max(1, len([m for m in away_form["last_10_matches"] if m["result"] in ["W","D","L"]]))

        derived = {
            "home_xg_per_game":  round(h_xg / h_matches, 3),
            "away_xg_per_game":  round(a_xg / a_matches, 3),
            "home_form_pts":     home_form["pts_last5"],
            "away_form_pts":     away_form["pts_last5"],
            "home_avg_xg_last5": home_form["avg_xg_for_last5"],
            "away_avg_xg_last5": away_form["avg_xg_for_last5"],
            "home_avg_xga_last5":home_form["avg_xg_against_last5"],
            "away_avg_xga_last5":away_form["avg_xg_against_last5"],
            "home_press_succ":   home_stats["defense"].get("pressure_success"),
            "away_press_succ":   away_stats["defense"].get("pressure_success"),
            "home_psxg_diff":    home_stats["goalkeeper"].get("psxg_vs_ga"),  # GK overperformance
            "away_psxg_diff":    away_stats["goalkeeper"].get("psxg_vs_ga"),
            "home_pass_pct":     home_stats["passing"].get("pass_accuracy"),
            "away_pass_pct":     away_stats["passing"].get("pass_accuracy"),
        }

        return JSONResponse(content={
            "home": {
                "slug": home_slug,
                "stats": home_stats,
                "form":  home_form,
            },
            "away": {
                "slug": away_slug,
                "stats": away_stats,
                "form":  away_form,
            },
            "h2h":    h2h,
            "derived": derived,
            "generated_at": datetime.now().isoformat(),
            "data_source": "FBref.com (powered by StatsBomb)",
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Match analysis error: {e}")
        raise HTTPException(500, f"Error: {str(e)}")


@app.get("/standings/{league}")
async def get_standings(league: str):
    """
    Klasemen liga dari FBref.
    leagues: bundesliga, premier-league, la-liga, serie-a, ligue-1
    """
    cached = cache_get(f"standings_{league}")
    if cached:
        return JSONResponse(content=cached)

    if league not in LEAGUE_SLUGS:
        raise HTTPException(404, f"League '{league}' tidak tersedia. Pilihan: {list(LEAGUE_SLUGS.keys())}")

    league_id = LEAGUE_SLUGS[league]
    url = f"https://fbref.com/en/comps/{league_id}/2024-2025/{league.title()}-Stats"

    try:
        soup = await fetch_page(url)
        tables = soup.find_all("table", {"id": re.compile(r"results.*overall")})
        if not tables:
            raise HTTPException(500, "Tabel klasemen tidak ditemukan")

        df = pd.read_html(str(tables[0]))[0]
        df.columns = [c[1] if isinstance(c, tuple) else c for c in df.columns]

        standings = []
        for _, row in df.iterrows():
            team = str(row.get("Squad", ""))
            if not team or team in ["nan", "Squad"]:
                continue
            standings.append({
                "rank":   parse_float(str(row.get("Rk", ""))),
                "team":   team,
                "mp":     parse_float(str(row.get("MP", ""))),
                "wins":   parse_float(str(row.get("W", ""))),
                "draws":  parse_float(str(row.get("D", ""))),
                "losses": parse_float(str(row.get("L", ""))),
                "gf":     parse_float(str(row.get("GF", ""))),
                "ga":     parse_float(str(row.get("GA", ""))),
                "gd":     str(row.get("GD", "")),
                "pts":    parse_float(str(row.get("Pts", ""))),
                "xg":     parse_float(str(row.get("xG", ""))),
                "xga":    parse_float(str(row.get("xGA", ""))),
                "pts_per_mp": parse_float(str(row.get("Pts/MP", ""))),
            })

        data = {"league": league, "standings": standings, "scraped_at": datetime.now().isoformat()}
        cache_set(f"standings_{league}", data)
        return JSONResponse(content=data)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Gagal scraping standings: {str(e)}")


@app.delete("/cache")
async def clear_cache():
    """Clear semua cache (admin endpoint)."""
    _cache.clear()
    return {"message": "Cache cleared", "timestamp": datetime.now().isoformat()}
