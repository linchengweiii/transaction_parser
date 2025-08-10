import argparse
from pathlib import Path
import json

from gmail_helper import GmailHelper
from cathay_us_trade_parser import CathayUSTradeParser


def main():
    ap = argparse.ArgumentParser(description="Fetch a PDF from Gmail and parse trades to CSV.")
    ap.add_argument("--credentials", type=Path, default=Path("credentials.json"),
                    help="OAuth client credentials JSON downloaded from Google Cloud.")
    ap.add_argument("--token", type=Path, default=Path("token.json"),
                    help="Cached OAuth token (created on first run).")
    ap.add_argument("--save-dir", type=Path, default=Path("downloads"),
                    help="Where to save downloaded PDFs.")
    ap.add_argument("--pdf-password", type=str, default=None,
                    help="PDF password, if any.")
    ap.add_argument("--out", type=Path, default=Path("output.json"),
                    help="Output JSON file to save parsed trades.")
    args = ap.parse_args()

    gmail = GmailHelper(args.credentials, args.token)
    cathay_us_parser = CathayUSTradeParser(
        gmail,
        args.save_dir,
        password=args.pdf_password,
        trace_back_days=100,
    )

    records = cathay_us_parser.parse()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Done. Parsed {len(records)} rows into {args.out.resolve()}")


if __name__ == "__main__":
    main()
