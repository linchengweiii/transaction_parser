import argparse
from pathlib import Path
import json
import os
from typing import List, Dict, Any, Optional

from gmail_helper import GmailHelper
from cathay_us_trade_parser import CathayUSTradeParser
from cathay_tw_trade_parser import CathayTWTradeParser
from schwab_trade_parser import SchwabTradeParser

from portfolio_client import PortfolioClient


from typing import Optional


def build_parsers(
    gmail: GmailHelper,
    save_dir: Path,
    pdf_password: Optional[str],
    trace_back_days: int,
    keep_artifacts: bool = False,
) -> dict:
    return {
        "cathay_us": CathayUSTradeParser(
            gmail=gmail,
            save_dir=save_dir / "cathay_us",
            password=pdf_password,
            trace_back_days=trace_back_days,
        ),
        "cathay_tw": CathayTWTradeParser(
            gmail=gmail,
            save_dir=save_dir / "cathay_tw",
            password=pdf_password,
            trace_back_days=trace_back_days,
        ),
        "schwab": SchwabTradeParser(
            gmail=gmail,
            save_dir=save_dir / "schwab",
            trace_back_days=trace_back_days,
            keep_artifacts=keep_artifacts,
        ),
    }


def run_single(parser_key: str, parsers: dict) -> List[Dict[str, Any]]:
    parser = parsers[parser_key]
    print(f"Running parser: {parser_key}")
    return parser.parse()


def push_records(client: PortfolioClient, portfolio_name: str, records: List[Dict[str, Any]]) -> Any:
    # Create/find portfolio, then upsert transactions
    portfolio = client.get_or_create_portfolio(portfolio_name)
    portfolio_id = (
        portfolio.get("id")
        or portfolio.get("portfolio_id")
        or portfolio.get("data", {}).get("id")
    )
    if not portfolio_id:
        raise RuntimeError(f"Unable to determine portfolio id from response: {portfolio}")
    return client.upsert_transactions(portfolio_id, records)


def main():
    ap = argparse.ArgumentParser(description="Fetch/parse trades from Gmail and optionally push to your portfolio API.")
    ap.add_argument("--credentials", type=Path, default=Path("credentials.json"),
                    help="OAuth client credentials JSON downloaded from Google Cloud.")
    ap.add_argument("--token", type=Path, default=Path("token.json"),
                    help="Cached OAuth token (created on first run).")
    ap.add_argument("--save-dir", type=Path, default=Path("downloads"),
                    help="Where to save downloaded artifacts (PDFs or message bodies).")
    ap.add_argument("--source", choices=["cathay_us", "cathay_tw", "schwab", "all"], default="cathay_us",
                    help="Which source to parse.")
    ap.add_argument("--pdf-password", dest="pdf_password", default=None,
                    help="Password for protected PDFs (TW/US if required).")
    ap.add_argument("--trace-back-days", type=int, default=100,
                    help="Limit Gmail search to newer_than:{days}d.")
    # Backwards-compatible alias (common typo): --trace-back-day
    ap.add_argument("--trace-back-day", dest="trace_back_days", type=int,
                    help="Alias for --trace-back-days.")

    # API settings
    ap.add_argument("--api-base", default=os.getenv("PORTFOLIO_API_BASE"),
                    help="Portfolio API base URL. Can also be set via PORTFOLIO_API_BASE environment variable.")
    ap.add_argument("--push", action="store_true",
                    help="If set, push parsed trades to the portfolio API; otherwise print JSON for debugging.")
    ap.add_argument("--keep-artifacts", action="store_true",
                    help="Keep downloaded/saved artifacts (HTML/TXT/PDF) for debugging.")

    args = ap.parse_args()

    # Gmail helper
    gmail = GmailHelper(credentials_path=args.credentials, token_path=args.token)

    parsers = build_parsers(
        gmail=gmail,
        save_dir=args.save_dir,
        pdf_password=args.pdf_password,
        trace_back_days=args.trace_back_days,
        keep_artifacts=args.keep_artifacts,
    )

    # Default portfolio names inferred from source
    default_portfolio_names = {
        "cathay_us": "CathayUS",
        "cathay_tw": "CathayTW",
        "schwab": "Schwab",
    }

    # Setup API client if pushing
    client = None
    if args.push:
        if not args.api_base:
            raise RuntimeError("Missing --api-base (or PORTFOLIO_API_BASE env).")
        client = PortfolioClient(base_url=args.api_base)

    if args.source == "all":
        total = 0
        for src in ["cathay_us", "cathay_tw", "schwab"]:
            records = run_single(src, parsers)
            pname = default_portfolio_names[src]
            # Add portfolio name as metadata on each record (harmless; server may ignore)
            for r in records:
                r["portfolio_name"] = pname

            if args.push:
                result = push_records(client, pname, records)
                print(f"[{src}] API response (truncated): {json.dumps(result, ensure_ascii=False)[:500]}")
            else:
                print(f"[{src}] Parsed {len(records)} record(s):")
                print(json.dumps(records, indent=2, ensure_ascii=False))
            total += len(records)
        print(f"Done. Parsed a total of {total} record(s) across all sources.")
    else:
        records = run_single(args.source, parsers)
        pname = default_portfolio_names[args.source]
        for r in records:
            r["portfolio_name"] = pname
        if args.push:
            result = push_records(client, pname, records)
            print(f"[{args.source}] API response (truncated): {json.dumps(result, ensure_ascii=False)[:500]}")
        else:
            print(json.dumps(records, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
