import logging
import time
import uuid
from typing import List, Optional

import httpx
from httpx import Response, Request

from model import Company, MeetingWithVotes
from nbimvr import NBIMVR


class NBIMVRClient(NBIMVR):
    def __init__(self, api_key: str) -> None:
        super().__init__(api_key)

        self.client = httpx.Client(
            headers=self.headers,
            event_hooks={
                "request": [self.__request_hook],
                "response": [self.__response_hook]
            }
        )

    def __request_hook(self, request: Request) -> None:
        request_id = str(uuid.uuid4())
        request.headers.update({"request-id": request_id})
        self.request_timer_dict[request_id] = time.time()

    def __response_hook(self, response: Response) -> None:
        request = response.request
        request_id = request.headers.get("request-id")
        request_time = (time.time() - self.request_timer_dict[request_id]).__round__(3)
        logging.info(f"[NBIMVR][{request.method}] {request.url} status:{response.status_code} time:{request_time} ms")

    def get_tickers(self) -> List[str]:
        response = self.client.get(f"{self.base_url}/ds/tickers")
        return [x["t"] for x in response.json()["dstickers"]["companies"]]

    def get_company_names(self) -> List[str]:
        response = self.client.get(f"{self.base_url}/ds/companies")
        return [x["n"] for x in response.json()["dscompanies"]["companies"]]

    def query_companies_with_ticker(self, ticker: str) -> List[Company]:
        response = self.client.get(f"{self.base_url}/query/ticker/{ticker}")
        return [Company.from_dict(company_dict) for company_dict in response.json()["companies"]]

    def query_company_with_name(self, name: str) -> List[Company]:
        response = self.client.get(f"{self.base_url}/query/company/{name}")
        return [Company.from_dict(company_dict) for company_dict in response.json()["companies"]]

    def query_company_with_id(self, id: int) -> Optional[Company]:
        response = self.client.get(f"{self.base_url}/query/companyid/{id}")
        company_dict = response.json()["companies"]
        if company_dict == "":
            return None
        return Company.from_dict(company_dict)

    def query_company_with_isin(self, isin: str) -> Optional[Company]:
        response = self.client.get(f"{self.base_url}/query/isin/{isin}")
        company_dict = response.json()["companies"]
        if company_dict == "":
            return None
        return Company.from_dict(company_dict)

    def get_meeting(self, id: int) -> Optional[MeetingWithVotes]:
        response = self.client.get(f"{self.base_url}/query/meeting/{id}")
        meeting_dict = response.json()["meeting"]
        if meeting_dict == "":
            return None
        return MeetingWithVotes.from_dict(meeting_dict)
