from typing import List, Dict, Any
import requests


class PortfolioClient:
    """
    REST client for the portfolio service.

    Expected endpoints (adjust base path via constructor if needed):
      - GET   {base}/portfolios
      - POST  {base}/portfolios                 body: {"name": "<portfolio name>"}
      - POST  {base}/portfolios/{id}/transactions
            Accepts either:
              a) {"transactions": [ ... ]}  (preferred bulk form), OR
              b) [ ... ]                     (raw list), OR
              c) single-object POST per transaction (fallback loop)
    """

    def __init__(self, base_url: str, timeout: int = 20):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    # ---------- portfolios ----------

    def list_portfolios(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base}/portfolios", timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("items", "data", "portfolios"):
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    def create_portfolio(self, name: str) -> Dict[str, Any]:
        r = requests.post(f"{self.base}/portfolios", json={"name": name}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_or_create_portfolio(self, name: str) -> Dict[str, Any]:
        for p in self.list_portfolios():
            if p.get("name") == name:
                return p
        return self.create_portfolio(name)

    # ---------- transactions ----------

    def upsert_transactions(self, portfolio_id: str, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        POST to /portfolios/{id}/transactions with smart fallbacks:
          1) Try {"transactions": [...]}
          2) Try raw list: [...]
          3) Fallback to posting each transaction individually (loop)
        Returns the successful response or a summary dict if falling back.
        """
        url = f"{self.base}/portfolios/{portfolio_id}/transactions"

        # 1) Preferred: wrapper object
        try:
            r = requests.post(url, json={"transactions": transactions}, timeout=self.timeout)
            if r.status_code < 400:
                return r.json()
            # If server explicitly doesn't like wrapper, continue to fallback
        except requests.RequestException:
            pass

        # 2) Raw list
        try:
            r = requests.post(url, json=transactions, timeout=self.timeout)
            if r.status_code < 400:
                return r.json()
        except requests.RequestException:
            pass

        # 3) One-by-one fallback
        results: List[Any] = []
        errors: List[Dict[str, Any]] = []
        for t in transactions:
            try:
                rr = requests.post(url, json=t, timeout=self.timeout)
                if rr.status_code < 400:
                    # Some APIs return the created transaction, others return {id: ...} or 204
                    try:
                        results.append(rr.json())
                    except ValueError:
                        results.append({"status": rr.status_code})
                else:
                    errors.append({"transaction": t, "status": rr.status_code, "text": rr.text[:300]})
            except requests.RequestException as ex:
                errors.append({"transaction": t, "error": str(ex)[:300]})
        return {
            "mode": "per-item-fallback",
            "submitted": len(transactions),
            "succeeded": len(results),
            "failed": len(errors),
            "sample_results": results[:10],
            "sample_errors": errors[:10],
        }
