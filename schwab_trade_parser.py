from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
import base64
import re
import shutil

from bs4 import BeautifulSoup  # requires bs4 at runtime

from trade_parser import TradeParser
from gmail_helper import GmailHelper


class SchwabTradeParser(TradeParser):
    
    """
    Parses Charles Schwab eConfirm emails.
    - Searches Gmail for eConfirms
    - Extracts HTML/text bodies
    - Parses symbol, action (buy/sell), shares, price, fee, settlement date, total
    Returns a list of JSON records compatible with the other parsers.
    """
    DEFAULT_QUERY = 'from:(donotreply@mail.schwab.com) eConfirms'

    def __init__(
        self,
        gmail: GmailHelper,
        save_dir: Union[Path, str],
        trace_back_days: Optional[int] = None,
        keep_artifacts: bool = False,
    ) -> None:
        self.gmail = gmail
        self.save_dir = Path(save_dir)
        self.keep_artifacts = keep_artifacts
        if trace_back_days is not None and trace_back_days > 0:
            self.query = f"{self.DEFAULT_QUERY} newer_than:{trace_back_days}d"
        else:
            self.query = self.DEFAULT_QUERY

    # ---------- public API ----------
    def parse(self) -> List[Dict[str, Any]]:
        """
        Search, fetch HTML bodies, parse them, and return normalized rows.
        """
        self.save_dir.mkdir(parents=True, exist_ok=True)
        msg_ids = self.gmail.search_messages("me", self.query)
        if not msg_ids:
            print("Schwab: No messages found matching query.")
            return []

        all_rows: List[Dict[str, Any]] = []
        for mid in msg_ids:
            html, text = self._get_message_bodies(mid)
            if not html and not text:
                # Last resort: dump raw and skip parsing
                raw = self.gmail.service.users().messages().get(
                    userId="me",
                    id=mid,
                    format="raw"
                ).execute()
                raw_bytes = base64.urlsafe_b64decode(raw.get("raw", "").encode("utf-8"))
                eml_path = GmailHelper._unique_path(self.save_dir / f"schwab_{mid}.eml")
                eml_path.write_bytes(raw_bytes)
                print(f"Schwab: saved raw .eml for message {mid} (no parseable body).")
                continue

            # Optionally save a copy for debugging
            if html:
                path = GmailHelper._unique_path(self.save_dir / f"schwab_{mid}.html")
                path.write_text(html, encoding="utf-8")
            elif text:
                path = GmailHelper._unique_path(self.save_dir / f"schwab_{mid}.txt")
                path.write_text(text, encoding="utf-8")

            body_text = html or text or ""
            rows = self._parse_body(body_text)
            all_rows.extend(rows)

        # remove the save_dir after successfully parsing
        if not self.keep_artifacts:
            try:
                shutil.rmtree(self.save_dir)
            except Exception as e:
                print(f"Warning: failed to remove temp dir {self.save_dir}: {e}")

        return all_rows

    # ---------- internals ----------
    def _get_message_bodies(self, msg_id: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Return (html, text) bodies for the message, if present.
        Prefers parts labeled 'text/html' or 'text/plain'. Decodes base64url.
        """
        msg = self.gmail.service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full"
        ).execute()
        payload = msg.get("payload", {}) or {}

        html: Optional[str] = None
        text: Optional[str] = None

        # Walk all parts (handles nested multiparts)
        for part in GmailHelper._walk_parts(payload):
            mime = (part.get("mimeType") or "").lower()
            if mime not in ("text/html", "text/plain"):
                continue
            body = (part.get("body") or {})
            data = body.get("data")
            if not data:
                # If attachmentId is present for a text part (rare), fetch it
                att_id = body.get("attachmentId")
                if att_id:
                    att = (
                        self.gmail.service.users()
                        .messages()
                        .attachments()
                        .get(userId="me", messageId=msg_id, id=att_id)
                        .execute()
                    )
                    data = att.get("data")
            if not data:
                continue

            try:
                raw = base64.urlsafe_b64decode(data.encode("utf-8"))
                decoded = raw.decode("utf-8", errors="replace")
            except Exception:
                continue

            if mime == "text/html":
                html = (html or "") + decoded
            elif mime == "text/plain":
                text = (text or "") + decoded

        # Some messages may place the body directly at the top level (no parts)
        if not html and not text:
            body = (payload.get("body") or {})
            data = body.get("data")
            if data:
                try:
                    raw = base64.urlsafe_b64decode(data.encode("utf-8"))
                    decoded = raw.decode("utf-8", errors="replace")
                    if "<html" in decoded.lower() or "</p>" in decoded.lower():
                        html = decoded
                    else:
                        text = decoded
                except Exception:
                    pass

        return html, text

    # --------- parsing helpers ---------
    def _parse_body(self, html_or_text: str) -> List[Dict[str, Any]]:
        """
        Parse one eConfirm body (HTML or text) into zero or more trade rows.
        Handles typical Schwab eConfirm structure containing:
          Symbol: <SYM> ... Trade Date: mm/dd/yy Settle Date: mm/dd/yy
          Quantity Price Principal Charge and/or Interest Total Amount
        """
        # Convert HTML to text if needed
        if "<" in html_or_text and "</" in html_or_text:
            soup = BeautifulSoup(html_or_text, "html.parser")
            text = soup.get_text(" ", strip=True)
        else:
            text = " ".join(html_or_text.split())

        records: List[Dict[str, Any]] = []

        # Find each trade block by locating "Symbol:" and capturing the following token(s)
        # up to the next field label (e.g., Security Description/Action/Type/Trade Date)
        sym_iter = re.finditer(
            r"Symbol:\s*(?P<sym>.+?)\s+(Security Description:|Action:|Type:|Trade Date:)",
            text,
        )
        for m in sym_iter:
            raw_sym = (m.group("sym") or "").strip()
            # Parse within a localized window (grow a bit for safety)
            window = text[m.start(): m.start() + 2000]

            # Convert Schwab option symbol to OCC-style if applicable
            sym = self._normalize_symbol(raw_sym)


            # Action (buy/sell)
            action = None
            # Handle Purchase/Buy/Bought and Sale/Sell/Sold variants
            mp = re.search(r"\b(Purchase|Buy|Bought)\b", window, re.I)
            ms = re.search(r"\b(Sale|Sell|Sold)\b", window, re.I)
            if mp and ms:
                action = "sell" if ms.start() <= mp.start() else "buy"
            elif ms:
                action = "sell"
            elif mp:
                action = "buy"
            # Dates
            trade_date = self._search(window, r"Trade Date:\s*([0-9]{2}/[0-9]{2}/[0-9]{2,4})")
            settle_date = self._search(window, r"Settle Date:\s*([0-9]{2}/[0-9]{2}/[0-9]{2,4})")

            # Table numbers: Quantity Price Principal Charge and/or Interest Total Amount
            qty = price = principal = fee = total = None
            # Prefer robust numeric extraction for the row beneath the headers.
            qty, price, principal, total = self._extract_row_numbers(window)
            # Extract fee from the Charge column when present (Commission, Industry Fee, etc.)
            fee_from_block = self._extract_fee_from_window(window)
            if fee_from_block is not None:
                fee = fee_from_block
            # If still unknown, infer fee as the difference between total and principal
            if fee is None and principal is not None and total is not None:
                try:
                    fee = round(abs(total) - abs(principal), 2)
                except Exception:
                    pass

            if sym and qty is not None and price is not None and (total is not None or principal is not None):
                # Compute cash amount prioritizing parsed total; otherwise derive from principal +/- fee by action
                amount = total if total is not None else None
                if amount is None and principal is not None:
                    # If we have principal and fee, derive total based on action semantics
                    eff_fee = fee if fee is not None else 0.0
                    if (action or '').lower() == 'sell':
                        amount = round(principal - eff_fee, 2)
                    else:
                        # Default and for buys: cash out equals principal + fees
                        amount = round(principal + eff_fee, 2)
                # Normalize cash flow sign: buys are cash out (negative), sells are cash in (positive)
                if amount is not None and action:
                    if action.lower() == "buy" and amount > 0:
                        amount = -amount
                    elif action.lower() == "sell" and amount < 0:
                        amount = -amount

                rec = {
                    "symbol": sym,
                    "trade_type": action or "",   # 'buy' or 'sell' when available
                    "currency": "USD",
                    "shares": qty,
                    "price": price,
                    "fee": fee if fee is not None else 0.0,
                    "date": (self._normalize_date(settle_date) or self._normalize_date(trade_date) or ""),
                    "total": amount,
                }
                records.append(rec)

        return records

    @staticmethod
    def _extract_fee_from_window(window_text: str) -> Optional[float]:
        """
        Extract the Charge-and/or-Interest total for a single trade row from the
        flattened text window. Handles options emails where the column lists
        multiple items like Commission and Industry Fee with a 'Total: $X.XX'.

        Strategy (in order):
        1) Look for 'Charge and/or Interest ... Total: $X.XX' and return that value.
        2) Sum known fee components (Commission, Industry Fee, Regulatory/ORF, etc.).
        Returns None if nothing is confidently found.
        """
        if not window_text:
            return None

        # Normalize whitespace to simplify regex
        w = " ".join(window_text.split())

        # 1) Try to capture the explicit Total within the Charge column
        m_total = re.search(r"Charge and/or Interest.*?Total:\s*\$?([\d,]+\.\d{2})", w, re.I)
        if m_total:
            try:
                return float(m_total.group(1).replace(",", ""))
            except Exception:
                pass

        # 2) Sum individual fee components commonly present in Schwab options emails
        labels = [
            "Commission",
            "Industry Fee",
            "Regulatory Fee",
            "Options Regulatory Fee",
            "Transaction Fee",
            "Exchange Fee",
            "Fees",  # sometimes shown as a single "Fees: $x.xx"
        ]
        total_fee = 0.0
        found_any = False
        for label in labels:
            for m in re.finditer(rf"{label}:\s*\$?([\d,]+\.\d{2})", w, re.I):
                try:
                    amt = float(m.group(1).replace(",", ""))
                    total_fee += amt
                    found_any = True
                except Exception:
                    continue

        return total_fee if found_any else None

    @staticmethod
    def _extract_row_numbers(window_text: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        Extract (qty, price, principal, total_amount) from the table segment under the
        'Quantity Price Principal Charge and/or Interest Total Amount' headers.
        Works for both equities and options where the Charge column has sub-items.
        """
        if not window_text:
            return None, None, None, None

        # Normalize whitespace to a single line for regex simplicity
        w = " ".join(window_text.split())
        # Try to capture the span after the header labels into the first data row.
        # Stop early when common post-row phrases appear to avoid pulling unrelated numbers.
        stop_markers = [
            r"Additional information",
            r"If you have any questions",
            r"We will hold this",
            r"Schwab acted as your agent",
            r"Notice: All email",
            r"Thank you for investing",
        ]
        stop_regex = r"|".join(stop_markers)
        m = re.search(rf"Quantity\s+Price\s+Principal.*?(Total Amount|Net Amount)\s+(?P<tail>.+?)(?:{stop_regex}|$)", w, re.I)
        if not m:
            return None, None, None, None

        tail = m.group("tail")
        # Collect numeric tokens in order of appearance (qty can be integer; others money)
        tokens = re.findall(r"N/A|\([\$\d,]+(?:\.\d{2,4})?\)|-?\$[\d,]+\.\d{2,4}|-?[\d,]+(?:\.\d{2,4})?", tail)
        if len(tokens) < 4:
            return None, None, None, None

        qty = SchwabTradeParser._to_num(tokens[0])
        price = SchwabTradeParser._to_money(tokens[1])
        principal = SchwabTradeParser._to_money(tokens[2])
        total = None
        # Choose the last money-like token as Total Amount; ensure it looks like money
        for tok in reversed(tokens):
            val = SchwabTradeParser._to_money(tok)
            if val is not None and isinstance(val, float):
                total = val
                break
        return qty, price, principal, total

    @staticmethod
    def _to_occ_symbol(underlying: str, expiry: str, cp: str, strike: str) -> Optional[str]:
        """
        Build OCC-style symbol: ROOT + YYMMDD + C/P + strikePrice(8 digits, price*1000).
        Accepts expiry in mm/dd/yy or mm/dd/yyyy; strike as decimal string.
        Returns None if formatting fails.
        """
        try:
            # Normalize date to yymmdd
            import datetime as _dt
            for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                try:
                    dt = _dt.datetime.strptime(expiry, fmt)
                    break
                except Exception:
                    dt = None
            if dt is None:
                return None
            yymmdd = dt.strftime("%y%m%d")
            # Normalize strike -> integer with 3 decimal places
            s = strike.replace(",", "")
            strike_thou = int(round(float(s) * 1000))
            strike_part = f"{strike_thou:08d}"
            root = underlying.strip().upper().replace(" ", "")
            cp_ch = cp.upper()[0]
            return f"{root}{yymmdd}{cp_ch}{strike_part}"
        except Exception:
            return None

    def _normalize_symbol(self, raw_sym: str) -> str:
        """Return OCC-style for options; otherwise the raw equity symbol."""
        s = " ".join(raw_sym.split())  # normalize whitespace
        # Option patterns like: "FTNT 09/19/2025 77.00 C" or "AAPL 9/6/25 195 P"
        m = re.match(
            r"^(?P<root>[A-Z0-9\.\-]+)\s+(?P<exp>\d{1,2}/\d{1,2}/\d{2,4})\s+(?P<strike>[\d,]+(?:\.\d{1,4})?)\s+(?P<cp>[CP])$",
            s,
            re.I,
        )
        if m:
            occ = self._to_occ_symbol(m.group("root"), m.group("exp"), m.group("cp"), m.group("strike"))
            if occ:
                return occ
        return raw_sym

    @staticmethod
    def _to_money(s: Optional[str]) -> Optional[float]:
        if s is None:
            return None
        s = s.strip()
        if s.upper() == "N/A":
            return 0.0
        # Handle parentheses for negatives and optional CR/DR suffixes
        neg = False
        if s.startswith("(") and s.endswith(")"):
            neg = True
            s = s[1:-1]
        s = s.replace("CR", "").replace("DR", "")
        s = s.replace("$", "").replace(",", "").strip()
        try:
            val = float(s)
            return -val if neg else val
        except Exception:
            return None

    @staticmethod
    def _to_num(s: Optional[str]) -> Optional[float]:
        if s is None:
            return None
        s = s.strip()
        neg = False
        if s.startswith("(") and s.endswith(")"):
            neg = True
            s = s[1:-1]
        s = s.replace(",", "")
        try:
            val = float(s)
            return -val if neg else val
        except Exception:
            return None

        
    @staticmethod
    def _normalize_date(s: Optional[str]) -> Optional[str]:
        """Return date as YYYY/MM/DD; accepts mm/dd/yy or mm/dd/yyyy."""
        if not s:
            return None
        import datetime as _dt
        s = s.strip()
        for fmt in ("%m/%d/%y", "%m/%d/%Y"):
            try:
                dt = _dt.datetime.strptime(s, fmt)
                if dt.year < 2000:
                    dt = dt.replace(year=dt.year + 100)
                return dt.strftime("%Y/%m/%d")
            except Exception:
                pass
        return s

    @staticmethod
    def _search(text: str, pattern: str) -> Optional[str]:
        m = re.search(pattern, text)
        return m.group(1) if m else None
