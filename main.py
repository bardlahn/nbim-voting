from fastapi import FastAPI, HTTPException
from client.nbimvr_client import NBIMVRClient

app = FastAPI()
client = NBIMVRClient()

@app.get("/companies/{name}")
def get_company_by_name(name: str):
    results = client.query_company_with_name(name)
    if not results:
        raise HTTPException(status_code=404, detail="No companies found")
    return results

@app.get("/companies/ticker/{ticker}")
def get_company_by_ticker(ticker: str):
    results = client.query_companies_with_ticker(ticker)
    if not results:
        raise HTTPException(status_code=404, detail="No companies found")
    return results

@app.get("/companies/id/{id}")
def get_company_by_id(id: int):
    result = client.query_company_with_id(id)
    if not result:
        raise HTTPException(status_code=404, detail="Company not found")
    return result

@app.get("/companies/isin/{isin}")
def get_company_by_isin(isin: str):
    result = client.query_company_with_isin(isin)
    if not result:
        raise HTTPException(status_code=404, detail="Company not found")
    return result

@app.get("/meetings/{id}")
def get_meeting(id: int):
    result = client.get_meeting(id)
    if not result:
        raise HTTPException(status_code=404, detail="Meeting not found")
    return result

@app.get("/tickers")
def get_tickers():
    return client.get_tickers()

@app.get("/company-names")
def get_company_names():
    return client.get_company_names()