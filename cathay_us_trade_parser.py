from pathlib import Path
import re
from typing import Optional, List, Dict, Any
import pdfplumber
import shutil

from trade_parser import TradeParser
from gmail_helper import GmailHelper


class CathayUSTradeParser(TradeParser):
    CURRENCIES = {"USD", "TWD"}
    TRADETYPE_MAP = {"買進": "buy", "賣出": "sell", "除息": "dividend"}

    def __init__(
        self,
        gmail: GmailHelper,
        save_dir: Path,
        password: Optional[str] = None,
        trace_back_days: int = 1,
    ):
        self.gmail = gmail
        self.query = " ".join([
            "from:e-notification@ebill1.cathaysec.com.tw",
            "has:attachment",
            "客戶買賣報告書",
            f"newer_than:{trace_back_days}d",
        ])
        self.save_dir = Path(save_dir)
        self.filename_contains = "客戶買賣報告書"
        self.password = password

    # ---------- public API ----------
    def parse(self) -> List[Dict[str, Any]]:
        """
        1) Search Gmail, download matching attachments to save_dir.
        2) Parse all PDFs found into normalized JSON rows.
        3) Return the list of rows.
        """
        self.save_dir.mkdir(parents=True, exist_ok=True)

        msg_ids = self.gmail.search_messages("me", self.query)
        if not msg_ids:
            print("No messages found matching query.")
            return []

        downloaded: List[str] = []
        for mid in msg_ids:
            files = self.gmail.download_attachments(
                "me", mid, self.save_dir, filename_contains=self.filename_contains
            )
            downloaded.extend([str(p) for p in files])

        if not downloaded:
            print("No matching attachments downloaded.")
            return []

        all_rows: List[Dict[str, Any]] = []
        for fpath in downloaded:
            if not fpath.lower().endswith(".pdf"):
                continue
            pdf_rows = self._parse_single_pdf(Path(fpath))
            all_rows.extend(pdf_rows)

        # remove the save_dir after successfully parsing
        try:
            shutil.rmtree(self.save_dir)
        except Exception as e:
            print(f"Warning: failed to remove temp dir {self.save_dir}: {e}")

        return all_rows

    # ---------- single-PDF parsing ----------
    def _parse_single_pdf(self, pdf_path: Path) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        with pdfplumber.open(pdf_path, password=self.password) as pdf:
            for page in pdf.pages:
                out.extend(self._parse_page(page))
        return out

    # ---------- page parsing ----------
    def _parse_page(self, page) -> List[Dict[str, Any]]:
        words = page.extract_words(
            x_tolerance=1,
            y_tolerance=1,
            keep_blank_chars=False,
            extra_attrs=["size", "fontname"],
        )

        header = [w for w in words if "TradeReference" in w["text"] or "交易序號" in w["text"]]
        if not header:
            return []

        header_bottom = max(w["bottom"] for w in header)
        disclaimers = [w for w in words if w["text"].startswith("重要事項") or "Important" in w["text"]]
        region_bottom = min([w["top"] for w in disclaimers], default=page.height) - 5
        region_words = [w for w in words if header_bottom + 2 <= w["top"] <= region_bottom]

        lines = self._cluster_rows(region_words, y_tol=2.5)

        recs, i = [], 0
        while i < len(lines):
            if re.match(r"^\d{8}\b", lines[i]):
                A = self._parse_rowA(lines[i]) or {}
                B = self._parse_rowB(lines[i + 1]) if i + 1 < len(lines) else {}
                _ = self._parse_rowC(lines[i + 2]) if i + 2 < len(lines) else {}

                merged = {}
                merged.update(A)
                merged.update(B)
                recs.append(self._project_row(merged))
                i += 3
            else:
                i += 1
        return [r for r in recs if r]

    # ---------- row parsers ----------
    def _parse_rowA(self, s: str) -> Optional[Dict[str, Any]]:
        m_ref = re.match(r"^(?P<ref>\d{8})\s+(?P<rest>.+)$", s)
        if not m_ref:
            return None
        rest = m_ref.group("rest")
        toks = rest.split()

        cur_idx = None
        for i, t in enumerate(toks):
            if re.fullmatch(r"[A-Z]{3}", t) and t in self.CURRENCIES:
                cur_idx = i

        if cur_idx is None:
            product_full = rest
            product = product_full.split("/", 1)[0] if "/" in product_full else product_full
            return {"Product": product}

        currency = toks[cur_idx]
        price = toks[cur_idx + 1] if cur_idx + 1 < len(toks) else ""
        arp = toks[cur_idx + 2] if cur_idx + 2 < len(toks) else ""
        product_full = " ".join(toks[:cur_idx])
        product = product_full.split("/", 1)[0] if "/" in product_full else product_full

        return {
            "Product": product,
            "Currency": currency,
            "Price": price,
            "Acct Receivable/Payable": arp,
        }

    @staticmethod
    def _parse_rowB(s: str) -> Dict[str, Any]:
        toks = s.split()
        out = {
            "Market": toks[0] if toks else "",
            "TradeType": toks[1] if len(toks) > 1 else "",
        }
        date = None
        for t in reversed(toks):
            if re.fullmatch(r"\d{4}/\d{2}/\d{2}", t):
                date = t
                break
        out["SettlementDate"] = date

        nums = [t for t in toks if re.fullmatch(r"-?[\d,]+(?:\.\d+)?", t)]
        out["Shares"] = nums[0] if len(nums) > 0 else ""
        out["Amount"] = nums[1] if len(nums) > 1 else ""
        out["HandlingFee"] = nums[2] if len(nums) > 2 else ""
        return out

    @staticmethod
    def _parse_rowC(s: str) -> Dict[str, Any]:
        toks = s.split()
        cur = toks[0] if toks and re.fullmatch(r"[A-Z]{3}", toks[0]) else ""
        nums = [t for t in toks if re.fullmatch(r"-?[\d,]+(?:\.\d+)?", t)]
        exr = nums[0] if len(nums) >= 1 else ""
        act = nums[1] if len(nums) >= 2 else ""
        return {"ActualCurrency": cur, "ExchangeRate": exr, "Actual Acct Rec/Pay": act}

    # ---------- projection ----------
    def _project_row(self, r: Dict[str, Any]) -> Dict[str, Any]:
        trade_type = self.TRADETYPE_MAP.get(r.get("TradeType", ""), r.get("TradeType", ""))

        return {
            "symbol": r.get("Product", ""),  # symbol replaces product
            "trade_type": trade_type,
            "currency": r.get("Currency", ""),
            "shares": self._to_num(r.get("Shares")),
            "price": self._to_num(r.get("Price")),
            "fee": self._to_num(r.get("HandlingFee")),
            "date": r.get("SettlementDate", ""),
            "total": self._to_num(r.get("Acct Receivable/Payable")),
        }

    # ---------- utils ----------
    @staticmethod
    def _cluster_rows(words, y_tol: float = 2.5) -> List[str]:
        rows = []
        for w in sorted(words, key=lambda w: w["top"]):
            placed = False
            for row in rows:
                if abs(row["y"] - w["top"]) <= y_tol:
                    row["words"].append(w)
                    row["y"] = (row["y"] * row["n"] + w["top"]) / (row["n"] + 1)
                    row["n"] += 1
                    placed = True
                    break
            if not placed:
                rows.append({"y": w["top"], "n": 1, "words": [w]})
        lines = []
        for row in rows:
            cells = sorted(row["words"], key=lambda w: w["x0"])
            text = " ".join(w["text"] for w in cells)
            lines.append(" ".join(text.split()))
        return lines

    @staticmethod
    def _to_num(x):
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return x
        s = str(x).replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None
