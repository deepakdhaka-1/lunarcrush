#!/usr/bin/env python3
"""
LunarCrush -> Google Sheets updater (sequential fetch & write)

What I changed:
- Kept all behavior and fields from your previous 593-line all-in-one script.
- Modified the main run flow so tickers are processed sequentially: for each ticker the script
  fetches its JSON, updates headers if new dynamic keys (change_intervals, metric_trends etc.)
  are discovered, and then immediately writes/updates that ticker row in the sheet.
- Headers are preserved (existing columns kept) and any newly discovered required headers are
  appended to the right before writing the current ticker row.
- raw_json fallback and oversize-cell handling remain the same.
- Token capture, logging, and Playwright usage unchanged.

Drop this file next to creds.json and run as before. It will now perform "fetch 1 -> wrote 1", "fetch 2 -> wrote 2", etc.
"""

from datetime import datetime
import time
import json
import traceback
from typing import Any, Dict, List, Set, Optional

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
from colorama import Fore, Style, init

# ---------- CONFIG ----------
LUNARCRUSH_START_URL = "https://lunarcrush.com/categories/cryptocurrencies"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1OmY0NEVPOeo-B4VCuKaTW9zHabqdlhQO7p4Ndj_g-H8/edit?gid=0#gid=0"
TICKER_SHEET_NAME = "Tickers"
DATA_SHEET_NAME = "cryptocurrencies"
CREDS_FILE = "creds.json"
LOGFILE = "log.txt"

MAX_CELL_LENGTH = 50000  # skip any single cell longer than this
SLEEP_TIME = 20 * 60  # 20 minutes
TOKEN_CAPTURE_TIMEOUT_MS = 60_000

init(autoreset=True)


# ---------- Logging ----------
def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def log_event(msg: str):
    with open(LOGFILE, "a") as f:
        f.write(f"[{_now()}] {msg}\n")
    print(Fore.GREEN + f"[{_now()}] " + Style.RESET_ALL + msg)


def log_warn(msg: str):
    with open(LOGFILE, "a") as f:
        f.write(f"[{_now()}] WARNING: {msg}\n")
    print(Fore.YELLOW + f"[{_now()}] WARNING: " + Style.RESET_ALL + msg)


def log_error(msg: str):
    with open(LOGFILE, "a") as f:
        f.write(f"[{_now()}] ERROR: {msg}\n")
    print(Fore.RED + f"[{_now()}] ERROR: " + Style.RESET_ALL + msg)


# ---------- Helpers ----------
def number_to_column(n: int) -> str:
    if n < 1:
        raise ValueError("n must be >= 1")
    letters = []
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def is_too_large(s: str) -> bool:
    return len(s) > MAX_CELL_LENGTH


def get_nested(d: dict, path: List[str], default=None):
    try:
        cur = d
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return default
        return cur
    except Exception:
        return default


# ---------- Playwright token capture (keeps original format) ----------
def capture_bearer_token(timeout_ms: int = TOKEN_CAPTURE_TIMEOUT_MS) -> Optional[str]:
    token_container = {"token": None}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            def on_response(resp):
                try:
                    headers = resp.request.headers
                    auth = headers.get("authorization") or headers.get("Authorization")
                    if auth and auth.startswith("Bearer "):
                        token_container["token"] = auth.split(" ", 1)[1]
                except Exception:
                    pass

            page.on("response", on_response)
            page.goto(LUNARCRUSH_START_URL, wait_until="domcontentloaded")

            end_time = time.time() + (timeout_ms / 1000.0)
            while time.time() < end_time and not token_container["token"]:
                page.wait_for_timeout(1000)

            browser.close()
    except Exception as ex:
        log_error(f"Playwright token capture failed: {ex}\n{traceback.format_exc()}")
    return token_container["token"]


# ---------- Google Sheets helpers ----------
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, scope)
    client = gspread.authorize(creds)
    return client


def open_workbook():
    client = get_gspread_client()
    return client.open_by_url(SPREADSHEET_URL)


def read_existing_headers(ws) -> List[str]:
    try:
        headers = ws.row_values(3)
        return headers or []
    except Exception as ex:
        log_warn(f"Could not read headers row: {ex}")
        return []


def append_new_headers(ws, current_headers: List[str], new_headers: List[str]) -> List[str]:
    missing = [h for h in new_headers if h not in current_headers]
    if not missing:
        return current_headers
    updated = current_headers + missing
    try:
        end_col = number_to_column(len(updated))
        range_str = f"A3:{end_col}3"
        ws.update(values=[updated], range_name=range_str)
        log_event(f"Appended {len(missing)} new headers: {missing}")
    except Exception as ex:
        log_error(f"Failed to append headers: {ex}")
        raise
    return updated


def find_row_for_ticker(ws, ticker: str, start_row: int = 4) -> Optional[int]:
    try:
        col_a = ws.col_values(1)
        for idx, v in enumerate(col_a[start_row - 1:], start=start_row):
            if v.strip().lower() == ticker.strip().lower():
                return idx
        return None
    except Exception as ex:
        log_warn(f"Failed searching ticker column: {ex}")
        return None


def write_row_by_header_order(ws, headers: List[str], values_map: Dict[str, Any], row_idx: int):
    row = []
    for h in headers:
        if h == "ticker":
            row.append(values_map.get("ticker", ""))
            continue
        v = values_map.get(h, "")
        s = safe_str(v)
        if is_too_large(s):
            log_warn(f"Skipping cell for header '{h}' at row {row_idx} due to length > {MAX_CELL_LENGTH}")
            row.append("")
        else:
            row.append(s)
    try:
        end_col = number_to_column(len(row))
        range_str = f"A{row_idx}:{end_col}{row_idx}"
        ws.update(values=[row], range_name=range_str)
        log_event(f"Wrote row at {range_str}")
    except Exception as ex:
        log_error(f"Failed to write row at {range_str}: {ex}")
        raise


# ---------- Metric_trends helpers (robust) ----------
def find_first_key_recursive(obj: Any, key: str):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = find_first_key_recursive(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first_key_recursive(item, key)
            if found is not None:
                return found
    return None


def pick_scalar_from_metric_obj(metric_obj: Any):
    if metric_obj is None:
        return None
    if isinstance(metric_obj, (int, float, str, bool)):
        return metric_obj
    if isinstance(metric_obj, dict):
        for c in ("value", "current", "count", "latest", "v", "val"):
            if c in metric_obj and isinstance(metric_obj[c], (int, float, str, bool)):
                return metric_obj[c]
        for k, v in metric_obj.items():
            if isinstance(v, (int, float, str, bool)):
                return v
        try:
            return json.dumps(metric_obj, ensure_ascii=False)
        except Exception:
            return str(metric_obj)
    if isinstance(metric_obj, list) and metric_obj:
        if isinstance(metric_obj[0], (int, float, str, bool)):
            return metric_obj[0]
        try:
            return json.dumps(metric_obj, ensure_ascii=False)
        except Exception:
            return str(metric_obj)
    return None


# ---------- Required fields (ensure present) ----------
REQUIRED_FIXED_HEADERS = [
    "ticker",
    # sentiment totals & counts under data
    "sentiment_positive_posts",
    "sentiment_positive_interactions",
    "sentiment_neutral_posts",
    "sentiment_neutral_interactions",
    "sentiment_negative_posts",
    "sentiment_negative_interactions",
    "posts_active",
    "posts_active_prev",
    "posts_created",
    "posts_created_prev",
    "contributors_active",
    "contributors_active_prev",
    "contributors_created",
    "contributors_created_prev",
    # alerts / ai / types
    "alerts",
    "ai_summary",
    "ai_summary_supportive",
    "types_count",
    "types_eng",
    "types_sentiment",
    "sentiment_types_tweet",
    "sentiment_types_youtube-video",
    "sentiment_types_tiktok-video",
    "sentiment_types_reddit-post",
    # asset flattened
    "asset_id",
    "asset_name",
    "asset_symbol",
    "asset_price",
    "asset_price_btc",
    "asset_market_cap",
    "asset_market_dominance",
    "asset_percent_change_1h",
    "asset_percent_change_24h",
    "asset_percent_change_7d",
    "asset_percent_change_30d",
    "asset_volume_24h",
    "asset_max_supply",
    "asset_circulating_supply",
    "asset_categories",
    "asset_close",
    "asset_interactions_24h",
    "asset_galaxy_score",
    "asset_alt_rank",
    "asset_volatility",
    "asset_market_cap_rank",
    "asset_social_dominance",
    "asset_price_all_time_high",
    "asset_price_all_time_high_date",
    "asset_price_52_week_high",
    "asset_price_52_week_high_date",
    "asset_price_52_week_low",
    "asset_price_52_week_low_date",
    # other top-level
    "interactions_24h",
    "interactions_24h_prev",
    "whatsup",
]

METRIC_TRENDS_KEYS = [
    "contributors_active", "contributors_created", "interactions", "posts_active", "posts_created",
    "sentiment", "spam", "alt_rank", "circulating_supply", "close", "galaxy_score", "market_cap",
    "market_dominance", "social_dominance", "volume_24h"
]


# ---------- Fetch wrapper ----------
def fetch_ticker_json(token: str, ticker: str) -> Optional[dict]:
    url = f"https://lunarcrush.com/api3/storm/topic/{ticker}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0",
        "X-Lunar-Client": "yolo",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as ex:
        log_error(f"Failed to fetch JSON for {ticker}: {ex}")
        return None


# ---------- Build change_intervals keys across multiple JSONs helper ----------
def gather_change_interval_keys_from_json(j: dict) -> List[str]:
    ci = get_nested(j, ["data", "change_intervals"], default={})
    if not isinstance(ci, dict):
        return []
    return list(ci.keys())


# ---------- Build values map for a ticker ----------
def build_values_map_for_ticker(j: dict, change_interval_keys: List[str]) -> Dict[str, Any]:
    m: Dict[str, Any] = {}

    # sentiment totals & posts/contributors counts under data
    for key in [
        "sentiment_positive_posts", "sentiment_positive_interactions",
        "sentiment_neutral_posts", "sentiment_neutral_interactions",
        "sentiment_negative_posts", "sentiment_negative_interactions",
        "posts_active", "posts_active_prev", "posts_created", "posts_created_prev",
        "contributors_active", "contributors_active_prev", "contributors_created", "contributors_created_prev"
    ]:
        m[key] = get_nested(j, ["data", key])

    # alerts / ai_summary
    m["alerts"] = get_nested(j, ["data", "alerts"], default=[])
    m["ai_summary"] = get_nested(j, ["data", "ai_summary"], default={})
    m["ai_summary_supportive"] = (m["ai_summary"].get("supportive") if isinstance(m["ai_summary"], dict) else [])

    # types
    m["types_count"] = get_nested(j, ["data", "types_count"], default={})
    m["types_eng"] = get_nested(j, ["data", "types_eng"], default={})
    m["types_sentiment"] = get_nested(j, ["data", "types_sentiment"], default={})

    # sentiment_types variants
    st = get_nested(j, ["data", "sentiment_types"], default={})
    if isinstance(st, dict):
        m["sentiment_types_tweet"] = st.get("tweet", {})
        m["sentiment_types_youtube-video"] = st.get("youtube-video", {})
        m["sentiment_types_tiktok-video"] = st.get("tiktok-video", {})
        m["sentiment_types_reddit-post"] = st.get("reddit-post", {})
    else:
        m["sentiment_types_tweet"] = {}
        m["sentiment_types_youtube-video"] = {}
        m["sentiment_types_tiktok-video"] = {}
        m["sentiment_types_reddit-post"] = {}

    # asset-level flattened
    asset = j.get("asset") or get_nested(j, ["data", "asset"], default={})
    m["asset_id"] = get_nested(asset, ["id"])
    m["asset_name"] = get_nested(asset, ["name"])
    m["asset_symbol"] = get_nested(asset, ["symbol"])
    m["asset_price"] = get_nested(asset, ["price"])
    m["asset_price_btc"] = get_nested(asset, ["price_btc"])
    m["asset_market_cap"] = get_nested(asset, ["market_cap"])
    m["asset_market_dominance"] = get_nested(asset, ["market_dominance"])
    m["asset_percent_change_1h"] = get_nested(asset, ["percent_change_1h"])
    m["asset_percent_change_24h"] = get_nested(asset, ["percent_change_24h"])
    m["asset_percent_change_7d"] = get_nested(asset, ["percent_change_7d"])
    m["asset_percent_change_30d"] = get_nested(asset, ["percent_change_30d"])
    m["asset_volume_24h"] = get_nested(asset, ["volume_24h"])
    m["asset_max_supply"] = get_nested(asset, ["max_supply"])
    m["asset_circulating_supply"] = get_nested(asset, ["circulating_supply"])
    m["asset_categories"] = get_nested(asset, ["categories"])
    m["asset_close"] = get_nested(asset, ["close"])
    m["asset_interactions_24h"] = get_nested(asset, ["interactions_24h"])
    m["asset_galaxy_score"] = get_nested(asset, ["galaxy_score"])
    m["asset_alt_rank"] = get_nested(asset, ["alt_rank"])
    m["asset_volatility"] = get_nested(asset, ["volatility"])
    m["asset_market_cap_rank"] = get_nested(asset, ["market_cap_rank"])
    m["asset_social_dominance"] = get_nested(asset, ["social_dominance"])
    m["asset_price_all_time_high"] = get_nested(asset, ["price_all_time_high"])
    m["asset_price_all_time_high_date"] = get_nested(asset, ["price_all_time_high_date"])
    m["asset_price_52_week_high"] = get_nested(asset, ["price_52_week_high"])
    m["asset_price_52_week_high_date"] = get_nested(asset, ["price_52_week_high_date"])
    m["asset_price_52_week_low"] = get_nested(asset, ["price_52_week_low"])
    m["asset_price_52_week_low_date"] = get_nested(asset, ["price_52_week_low_date"])

    # interactions and support
    m["interactions_24h"] = get_nested(j, ["data", "interactions_24h"])
    m["interactions_24h_prev"] = get_nested(j, ["data", "interactions_24h_prev"])
    m["whatsup"] = get_nested(j, ["data", "whatsup"])

    # metric_trends extraction: try common locations, then recursive search
    mt = j.get("metric_trends") or get_nested(j, ["data", "metric_trends"])
    if mt is None:
        mt = find_first_key_recursive(j, "metric_trends")
    if isinstance(mt, dict):
        for k in METRIC_TRENDS_KEYS:
            raw = mt.get(k)
            m[f"metric_trends_{k}"] = pick_scalar_from_metric_obj(raw)
    else:
        # fallback: try to find suitable numbers from data.change_intervals (if present)
        ci = get_nested(j, ["data", "change_intervals"], default={})
        if isinstance(ci, dict):
            for k in METRIC_TRENDS_KEYS:
                m[f"metric_trends_{k}"] = ci.get(k) if k in ci else None
        else:
            for k in METRIC_TRENDS_KEYS:
                m[f"metric_trends_{k}"] = None

    # change_intervals keys (values)
    ci = get_nested(j, ["data", "change_intervals"], default={})
    for ck in change_interval_keys_local:
        if isinstance(ci, dict):
            m[f"change_intervals_{ck}"] = ci.get(ck)
        else:
            m[f"change_intervals_{ck}"] = None

    return m


# ---------- Globals used by builder ----------
change_interval_keys_local: List[str] = []


# ---------- Main run (sequential) ----------
def run_once():
    global change_interval_keys_local
    log_event("Starting run: capturing Bearer token (visible browser)...")
    token = capture_bearer_token(timeout_ms=TOKEN_CAPTURE_TIMEOUT_MS)
    if not token:
        log_error("Failed to capture token. Aborting run.")
        return

    try:
        wb = open_workbook()
    except Exception as ex:
        log_error(f"Could not open workbook: {ex}")
        return

    try:
        data_ws = wb.worksheet(DATA_SHEET_NAME)
    except Exception as ex:
        log_error(f"Could not open data sheet '{DATA_SHEET_NAME}': {ex}")
        return

    try:
        ticker_ws = wb.worksheet(TICKER_SHEET_NAME)
    except Exception as ex:
        log_error(f"Could not open tickers sheet '{TICKER_SHEET_NAME}': {ex}")
        return

    # Update token and timestamp in A2/B2
    try:
        data_ws.update(values=[[token]], range_name="A2")
        data_ws.update(values=[[datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")]], range_name="B2")
        log_event("Auth token and timestamp updated in A2/B2.")
    except Exception as ex:
        log_warn(f"Failed to write token/timestamp: {ex}")

    # Read tickers from A4+
    raw_tickers = ticker_ws.col_values(1)
    tickers = [t.strip() for t in raw_tickers[1:] if t.strip()]
    if not tickers:
        log_warn("No tickers found in Tickers sheet (A4+). Exiting run.")
        return
    log_event(f"Tickers to process (sequential): {tickers}")

    # Read existing headers
    existing_headers = read_existing_headers(data_ws)
    if not existing_headers:
        # create initial header row with REQUIRED_FIXED_HEADERS + metric_trends + (no change_intervals yet) + raw_json
        headers = REQUIRED_FIXED_HEADERS.copy()
        for k in METRIC_TRENDS_KEYS:
            headers.append(f"metric_trends_{k}")
        headers.append("raw_json")
        try:
            end_col = number_to_column(len(headers))
            data_ws.update(values=[headers], range_name=f"A3:{end_col}3")
            log_event(f"Wrote initial headers ({len(headers)}) to sheet.")
        except Exception as ex:
            log_error(f"Failed to write initial headers: {ex}")
            return
    else:
        headers = existing_headers
        # ensure raw_json exists
        if "raw_json" not in headers:
            headers = append_new_headers(data_ws, headers, ["raw_json"])

    # Process tickers one by one: fetch -> possibly extend headers -> write row
    for idx, t in enumerate(tickers, start=1):
        log_event(f"Fetching JSON for ticker ({idx}/{len(tickers)}): {t}")
        j = fetch_ticker_json(token, t)
        if j is None:
            log_warn(f"No JSON for {t}; will append minimal row if not present.")
            row_idx = find_row_for_ticker(data_ws, t, start_row=4)
            if row_idx:
                log_event(f"Ticker {t} already exists at row {row_idx}; skipping.")
            else:
                append_row_idx = max(4, len(data_ws.col_values(1)) + 1)
                try:
                    data_ws.update(values=[[t] + [""] * (len(headers) - 1)], range_name=f"A{append_row_idx}:{number_to_column(len(headers))}{append_row_idx}")
                    log_event(f"Fetch {idx} wrote {idx} (minimal appended row).")
                except Exception as ex:
                    log_error(f"Failed to append minimal row for {t}: {ex}")
            continue

        # discover change_interval keys from this ticker JSON
        new_ci_keys = gather_change_interval_keys_from_json(j)
        # determine which discovered keys are truly new relative to local list
        new_to_add = [k for k in new_ci_keys if k not in change_interval_keys_local]
        if new_to_add:
            log_event(f"Discovered {len(new_to_add)} new change_intervals keys for ticker {t}: {new_to_add}")
            # add to local list (preserve order)
            change_interval_keys_local.extend(new_to_add)
            # prepare headers to append: change_intervals_<key> for each new key
            new_change_interval_headers = [f"change_intervals_{k}" for k in new_to_add]
            # Also ensure metric_trends columns exist (if user sheet didn't have them)
            needed_metric_headers = [f"metric_trends_{k}" for k in METRIC_TRENDS_KEYS if f"metric_trends_{k}" not in headers]
            headers = append_new_headers(data_ws, headers, needed_metric_headers + new_change_interval_headers)
        else:
            # still ensure metric_trends headers exist (first time)
            needed_metric_headers = [f"metric_trends_{k}" for k in METRIC_TRENDS_KEYS if f"metric_trends_{k}" not in headers]
            if needed_metric_headers:
                headers = append_new_headers(data_ws, headers, needed_metric_headers)

        # ensure raw_json header present
        if "raw_json" not in headers:
            headers = append_new_headers(data_ws, headers, ["raw_json"])

        # Build values_map using current change_interval_keys_local
        try:
            values_map = build_values_map_for_ticker(j, change_interval_keys_local)
        except Exception as ex:
            log_error(f"Failed to build values map for {t}: {ex}\n{traceback.format_exc()}")
            values_map = {}
        values_map["ticker"] = t

        # Write/update ticker row immediately
        row_idx = find_row_for_ticker(data_ws, t, start_row=4)
        try:
            if row_idx:
                # update in-place
                write_row_by_header_order(data_ws, headers, values_map, row_idx)
                log_event(f"Fetch {idx} wrote {idx} (updated row {row_idx})")
            else:
                # append new row
                append_row_idx = max(4, len(data_ws.col_values(1)) + 1)
                # build row aligned to headers
                row_vals = []
                for h in headers:
                    v = values_map.get(h, "")
                    s = safe_str(v)
                    if is_too_large(s):
                        log_warn(f"Cell for header '{h}' too large for ticker {t}; leaving blank.")
                        s = ""
                    row_vals.append(s)
                end_col = number_to_column(len(row_vals))
                range_str = f"A{append_row_idx}:{end_col}{append_row_idx}"
                data_ws.update(values=[row_vals], range_name=range_str)
                log_event(f"Fetch {idx} wrote {idx} (appended at row {append_row_idx})")
        except Exception as ex:
            log_error(f"Write failed for {t}: {ex}\nAttempting raw_json fallback.")
            try:
                values_map["raw_json"] = safe_str(j) if not is_too_large(safe_str(j)) else ""
                if row_idx:
                    write_row_by_header_order(data_ws, headers, values_map, row_idx)
                    log_event(f"Fetch {idx} wrote {idx} (raw_json fallback updated).")
                else:
                    append_row_idx = max(4, len(data_ws.col_values(1)) + 1)
                    row_vals = [safe_str(values_map.get(h, "")) if not is_too_large(safe_str(values_map.get(h, ""))) else "" for h in headers]
                    end_col = number_to_column(len(row_vals))
                    data_ws.update(values=[row_vals], range_name=f"A{append_row_idx}:{end_col}{append_row_idx}")
                    log_event(f"Fetch {idx} wrote {idx} (raw_json fallback appended).")
            except Exception as ex2:
                log_error(f"Final fallback failed for {t}: {ex2}")

        # tiny polite pause between tickers
        time.sleep(0.25)

    log_event("Run complete.")


def main():
    while True:
        try:
            run_once()
        except Exception as ex:
            log_error(f"Unhandled exception in run: {ex}\n{traceback.format_exc()}")
        log_event(f"Sleeping {SLEEP_TIME // 60} minutes...")
        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    main()
