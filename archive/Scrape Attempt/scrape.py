# scrape.py
import time, sys
import pandas as pd
import requests
from bs4 import BeautifulSoup

YEARS = list(range(2017, 2025))  # 2017–2024 inclusive
BASE = "https://www.fantasypros.com/nfl/reports/leaders/ppr.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}

def fetch_year(year: int) -> pd.DataFrame:
    url = f"{BASE}?year={year}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # Use BeautifulSoup to find the main table (more reliable than blind read_html)
    soup = BeautifulSoup(r.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError(f"No tables found for {year} at {url}")

    # Pick the widest table (usually the leaders grid)
    widest = max(tables, key=lambda t: len(t.find_all("th")))
    df = pd.read_html(str(widest))[0]

    # Normalize column names a bit and add year
    df.columns = [str(c).strip() for c in df.columns]
    df.insert(0, "Year", year)

    # Optional: standardize likely columns if present
    rename_map = {
        "Player": "Player",
        "Tm": "Team",
        "Pos": "Pos",
        "FPTS": "Points",
        "FPTS/G": "Points/G",
        "GP": "Games",
        "G": "Games",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Drop any multi-index header rows that sometimes sneak in
    df = df[df.columns].loc[df[df.columns[1]].notna()].reset_index(drop=True)

    return df

def main():
    all_dfs = []
    for y in YEARS:
        try:
            print(f"Fetching {y}…", file=sys.stderr)
            df = fetch_year(y)
            all_dfs.append(df)
            time.sleep(1.5)  # be polite
        except Exception as e:
            print(f"[WARN] {y}: {e}", file=sys.stderr)

    if not all_dfs:
        raise SystemExit("No data scraped.")

    out = pd.concat(all_dfs, ignore_index=True)
    # Light cleanup: remove rank symbols or stray unnamed cols
    out = out[[c for c in out.columns if "Unnamed" not in c]]
    out.to_csv("fantasypros_overall_leaders_ppr_2017_2024.csv", index=False)
    print("Wrote fantasypros_overall_leaders_ppr_2017_2024.csv")

if __name__ == "__main__":
    main()
