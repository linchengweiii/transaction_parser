from abc import ABC, abstractmethod
from typing import List, Dict, Any


class TradeParser(ABC):
    @abstractmethod
    def parse(self) -> List[Dict[str, Any]]:
        """Parse trades into a list of JSON-serializable dicts."""
        raise NotImplementedError
