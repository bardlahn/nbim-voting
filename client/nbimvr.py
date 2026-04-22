from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from collections import OrderedDict

from model import Company, MeetingWithVotes

class LRUDict(OrderedDict):
    def __init__(self, maxsize=128):
        super().__init__()
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

class NBIMVR(ABC):
    base_url: str = "https://vd.a.nbim.no/v1"

    def __init__(self, api_key: str) -> None:
        super().__init__()

        self.headers: Dict[str, str] = {"x-api-key": api_key}
        self.request_timer_dict = LRUDict(128)

    @abstractmethod
    def get_tickers(self) -> List[str]:
        pass

    @abstractmethod
    def get_company_names(self) -> List[str]:
        pass

    @abstractmethod
    def query_companies_with_ticker(self, ticker: str) -> List[Company]:
        pass

    @abstractmethod
    def query_company_with_name(self, name: str) -> List[Company]:
        pass

    @abstractmethod
    def query_company_with_id(self, id: int) -> Optional[Company]:
        pass

    @abstractmethod
    def query_company_with_isin(self, isin: str) -> Optional[Company]:
        pass

    @abstractmethod
    def get_meeting(self, id: int) -> Optional[MeetingWithVotes]:
        pass
