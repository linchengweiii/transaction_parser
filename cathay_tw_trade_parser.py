from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import re
import shutil
import pdfplumber

from trade_parser import TradeParser
from gmail_helper import GmailHelper


class CathayTWTradeParser(TradeParser):
    """
    Parser for 國泰綜合證券日對帳單 (Taiwan) PDF statements.
    1) Fetch attachments via Gmail.
    2) Parse trade rows and project to a common JSON schema used by other parsers.
    """
    DEFAULT_QUERY = (
        'from:(e-notification@ebill1.cathaysec.com.tw) '
        'subject:(國泰綜合證券日對帳單) '
        'has:attachment'
    )

    TRADETYPE_MAP = {
        "買進": "buy",
        "集買": "buy",
        "現股買進": "buy",
        "賣出": "sell",
        "集賣": "sell",
        "現股賣出": "sell",
    }

    def __init__(
        self,
        gmail: GmailHelper,
        save_dir: Union[Path, str],
        password: Optional[str] = None,
        trace_back_days: Optional[int] = None,
    ) -> None:
        self.gmail = gmail
        self.save_dir = Path(save_dir)
        self.password = password
        self.filename_contains = "國泰證券日對帳單"
        if trace_back_days is not None and trace_back_days > 0:
            self.query = f"{self.DEFAULT_QUERY} newer_than:{trace_back_days}d"
        else:
            self.query = self.DEFAULT_QUERY

    # --- Phase 1: attachment fetching ---
    def fetch_attachments(self) -> List[Path]:
        self.save_dir.mkdir(parents=True, exist_ok=True)
        msg_ids = self.gmail.search_messages("me", self.query)
        if not msg_ids:
            print("CathayTW: No messages found matching query.")
            return []
        downloaded: List[Path] = []
        for mid in msg_ids:
            files = self.gmail.download_attachments(
                user_id="me",
                msg_id=mid,
                save_dir=self.save_dir,
                filename_contains=self.filename_contains,
            )
            downloaded.extend(files)
        if not downloaded:
            print("CathayTW: No matching attachments downloaded.")
        else:
            print(f"CathayTW: downloaded {len(downloaded)} file(s) to {self.save_dir.resolve()}")
        return downloaded

    # --- TradeParser interface ---
    def parse(self) -> List[Dict[str, Any]]:
        paths = self.fetch_attachments()
        if not paths:
            return []
        all_rows: List[Dict[str, Any]] = []
        for p in paths:
            if not str(p).lower().endswith('.pdf'):
                continue
            try:
                all_rows.extend(self._parse_single_pdf(Path(p)))
            except Exception as e:
                print(f"Warning: failed to parse {p}: {e}")
        # optional cleanup similar to US parser
        try:
            shutil.rmtree(self.save_dir)
        except Exception as e:
            print(f"Warning: failed to remove temp dir {self.save_dir}: {e}")
        return all_rows

    # --- Single-PDF parsing ---
    def _parse_single_pdf(self, pdf_path: Path) -> List[Dict[str, Any]]:
        lines = self._extract_lines(pdf_path)
        settle_date = self._extract_settlement_date(lines)
        name_to_code = self._extract_code_mapping(lines)

        trades: List[Dict[str, Any]] = []
        i = 0
        while i < len(lines):
            s = lines[i]
            # Match a trade row (first seven columns); allow trailing columns we don't care about.
            m = re.match(
                r"^(?P<name>\S+)\s+(?P<tt>\S+)\s+(?P<shares>[\d,]+)\s+(?P<price>[\d,]+(?:\.\d+)?)\s+(?P<amt>[\d,]+)\s+(?P<fee>[\d,]+)\s+(?P<tax>[\d,]+)\b",
                s
            )
            if m:
                name = m.group("name")
                tt = m.group("tt")
                # Filter out summary or non-trade rows
                if name in ("總合計", "買進總計：", "賣出總計："):
                    i += 1
                    continue
                if tt not in self.TRADETYPE_MAP:
                    i += 1
                    continue

                shares = self._to_num(m.group("shares"))
                price = self._to_num(m.group("price"))
                fee = self._to_num(m.group("fee"))
                # Try to get 客戶應收付額 (receivable/payable) from subsequent lines if needed.
                rp_val = None
                for k in range(1, 4):
                    if i + k < len(lines):
                        m2 = re.match(r"^\s*(?P<num>-?[\d,]+)\s*$", lines[i + k])
                        if m2 and m2.group("num").strip() != "0":
                            rp_val = self._to_num(m2.group("num"))
                            i += k  # consume the used line(s)
                            break

                if shares is not None and price is not None and rp_val is not None:
                    # Map name -> code if available; append .TW for 4-digit codes.
                    symbol = name_to_code.get(name, name)
                    if re.fullmatch(r"\d{4}", symbol):
                        symbol = f"{symbol}.TW"
                    trade_type = self.TRADETYPE_MAP.get(tt, tt)
                    rec = {
                        "symbol": symbol,
                        "trade_type": trade_type,
                        "currency": "TWD",
                        "shares": shares,
                        "price": price,
                        "fee": fee,
                        "date": settle_date or "",
                        "total": rp_val,
                    }
                    trades.append(rec)
                i += 1
                continue
            i += 1

        return trades

    # --- helpers ---
    def _extract_lines(self, pdf_path: Path) -> List[str]:
        lines: List[str] = []
        with pdfplumber.open(str(pdf_path), password=self.password) as pdf:
            for page in pdf.pages:
                words = page.extract_words(x_tolerance=2, y_tolerance=2) or []
                # cluster words into rows by y and sort by x0
                rows: List[Dict[str, Any]] = []
                for w in words:
                    placed = False
                    for row in rows:
                        if abs(row["y"] - w["top"]) < 3:
                            row["words"].append(w)
                            row["y"] = (row["y"] * row["n"] + w["top"]) / (row["n"] + 1)
                            row["n"] += 1
                            placed = True
                            break
                    if not placed:
                        rows.append({"y": w["top"], "n": 1, "words": [w]})
                for row in rows:
                    cells = sorted(row["words"], key=lambda w: w["x0"])
                    text = " ".join(w["text"] for w in cells)
                    # normalize whitespace
                    lines.append(" ".join(text.split()))
        return lines

    def _extract_settlement_date(self, lines: List[str]) -> Optional[str]:
        # Search for header line containing 成交日期 / 交割日期, then read next line for actual dates
        for idx, s in enumerate(lines):
            if ("成交日期" in s and "交割日期" in s) and idx + 1 < len(lines):
                nxt = lines[idx + 1]
                m = re.search(r"(\d{4}/\d{2}/\d{2})\s+(\d{4}/\d{2}/\d{2})", nxt)
                if m:
                    # Use settlement date to align with US parser semantics
                    return m.group(2)
        return None

    def _extract_code_mapping(self, lines: List[str]) -> Dict[str, str]:
        """
        Parse the '代碼 股票名稱' section to map Chinese names -> stock codes.
        """
        mapping: Dict[str, str] = {}
        in_block = False
        for s in lines:
            if "代碼" in s and "股票名稱" in s:
                in_block = True
                continue
            if in_block:
                # Stop when we hit obvious footers or unrelated sections
                if not s.strip() or "集保市值總計" in s or "理財資訊" in s:
                    break
                # Typical row: "2330 台積電 ▼1,140 75 85,500 0 0 ..."
                m = re.match(r"^(?P<code>\d{4})\s+(?P<name>\S+)\b", s)
                if m:
                    mapping[m.group("name")] = m.group("code")
        return mapping

    @staticmethod
    def _to_num(x):
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return x
        s = str(x).replace(",", "").strip()
        try:
            if "." in s:
                return float(s)
            return int(s)
        except Exception:
            return None
