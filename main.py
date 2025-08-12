import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

from portfolio_client import PortfolioClient

from gmail_helper import GmailHelper
from cathay_us_trade_parser import CathayUSTradeParser
from cathay_tw_trade_parser import CathayTWTradeParser


def get_api_base(cli_value: Optional[str]) -> str:
    """
    Resolve API base URL from CLI or environment.
    Falls back to http://localhost:8080 if nothing provided.
    """
    base = cli_value or os.getenv("PORTFOLIO_API_BASE") or "http://localhost:8080"
    # Strip trailing slash for consistent join behavior
    return base.rstrip("/")


def parse_int(value: Optional[str], default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def build_gmail(args) -> GmailHelper:
    return GmailHelper(
        credentials_path=args.credentials,
        token_path=args.token,
    )


def run_cathay_us(gmail: GmailHelper, save_dir: Path, pdf_password: Optional[str], trace_back_days: int) -> List[Dict[str, Any]]:
    parser = CathayUSTradeParser(
        gmail,
        save_dir,
        password=pdf_password,
        trace_back_days=trace_back_days,
    )
    return parser.parse()


def run_cathay_tw(gmail: GmailHelper, save_dir: Path, pdf_password: Optional[str], trace_back_days: int) -> List[Dict[str, Any]]:
    parser = CathayTWTradeParser(
        gmail,
        save_dir,
        password=pdf_password,
        trace_back_days=trace_back_days,
    )
    return parser.parse()


def main():
    ap = argparse.ArgumentParser(description="Fetch trades from Gmail and push them to a portfolio service.")
    ap.add_argument("--credentials", type=Path, default=Path("credentials.json"),
                    help="OAuth client credentials JSON from Google Cloud.")
    ap.add_argument("--token", type=Path, default=Path("token.json"),
                    help="Cached OAuth token (created on first run).")
    ap.add_argument("--save-dir", type=Path, default=Path("downloads"),
                    help="Directory for saving downloaded PDFs (and any temp files).")
    ap.add_argument("--pdf-password", type=str, default=None,
                    help="Password for encrypted Cathay PDFs (if required).")
    ap.add_argument("--trace-back-days", type=int, default=parse_int(os.getenv("TRACE_BACK_DAYS"), 100),
                    help="How many days back to search in Gmail.")
    ap.add_argument("--api-base", type=str, default=None,
                    help="Portfolio service base URL. Defaults to PORTFOLIO_API_BASE env or http://localhost:8080.")
    ap.add_argument("--portfolio-name", type=str, default="CathayUS",
                    help="Target portfolio name to upsert transactions into.")
    ap.add_argument("--source", choices=["us", "tw"], default="tw",
                    help="Which parser/source to run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and print a sample, but do not call the API.")
    args = ap.parse_args()

    gmail = build_gmail(args)

    if args.source == "us":
        records = run_cathay_us(gmail, args.save_dir, args.pdf_password, args.trace_back_days)
    else:
        records = run_cathay_tw(gmail, args.save_dir, args.pdf_password, args.trace_back_days)

    if not records:
        print("No transactions parsed. Nothing to send.")
        return

    print(f"Parsed {len(records)} transaction(s) from Cathay {args.source.upper()}.")

    if args.dry_run:
        print(json.dumps(records[:3], ensure_ascii=False, indent=2))
        print("Dry-run: skipping API calls.")
        return

    base = get_api_base(args.api_base)
    client = PortfolioClient(base)

    portfolio = client.get_or_create_portfolio(args.portfolio_name)
    portfolio_id = portfolio.get("id") or portfolio.get("portfolio_id") or portfolio.get("uuid")
    if not portfolio_id:
        raise RuntimeError(f"Unable to determine portfolio id from response: {portfolio}")

    # Push transactions
    result = client.upsert_transactions(portfolio_id, records)
    # Be permissive about the response shape; just echo a summary.
    print("API response (truncated to 1KB):")
    response_str = json.dumps(result, ensure_ascii=False)[:1024]
    print(response_str)
    print(f"Done. Sent {len(records)} transaction(s) to portfolio '{args.portfolio_name}' ({portfolio_id}).")


if __name__ == "__main__":
    main()
