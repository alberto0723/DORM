import requests
import time
from pathlib import Path

"""
This module fetches filtered SQL logs from SDSS using the x_sql.asp endpoint.
It integrates pre-parsing filters (error=0, elapsed>0, rows>0) directly into the query
to improve performance. Previously, filtering was handled in a separate module.
"""

URL = "https://skyserver.sdss.org/log/en/traffic/x_sql.asp"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded"
}


def fetch_logs(year: int = None, month: int = None, day: int = None, limit: int = None):
    datestring = ""
    if limit is None:
        query = "SELECT "
    else:
        query = f"SELECT TOP {limit} "
    query += """ 
        theTime, 
        statement,
        elapsed,
        busy,
        [rows]
    FROM SqlLog
    WHERE
        error = 0 AND
        elapsed > 0 AND
        [rows] > 0"""
    if year is not None:
        datestring += f"{year}"
        query += f" AND datepart(yyyy, theTime) = {year}"
    if month is not None:
        datestring += f"_{str(month).zfill(2)}"
        query += f" AND datepart(mm, theTime) = {month}"
    if day is not None:
        datestring += f"_{str(day).zfill(2)}"
        query += f" AND datepart(dd, theTime) = {day}"

    query.replace("\n", " ").strip()

    data = {"cmd": query, "format": "csv"}

    print(f"📦 Fetching TOP {limit} logs from " + datestring + "...")
    start = time.time()
    response = requests.post(URL, data=data, headers=HEADERS)
    end = time.time()
    elapsed = round(end - start, 2)

    if response.status_code == 200:
        path = Path("data")
        path.mkdir(parents=True, exist_ok=True)
        if limit is None:
            filename = f"fetched_{datestring}_all.csv"
        else:
            filename = f"fetched_{datestring}_top{limit}.csv"
        file = path / filename
        with open(file, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"💾 Data saved to {file.absolute()} (⏱️ {elapsed}s)")
    else:
        print(f"❌ Failed with status {response.status_code}")
        print(response.text[:300])
