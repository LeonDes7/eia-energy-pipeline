import requests
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
BASE_URL = "https://api.eia.gov/v2"

def fetch_electricity_generation(start: str = "2015-01", end: str = "2024-12") -> list[dict]:
    url = f"{BASE_URL}/electricity/electric-power-operational-data/data/"
    results = []
    offset = 0

    while True:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "generation",
            "facets[fueltypeid][]": ["COL", "NG", "NUC", "SUN", "WND", "HYC"],
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": 5000
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("data", [])

        if not data:
            break

        for row in data:
            results.append({
                "source": "eia_electricity",
                "series_id": f"elec_{row.get('location')}_{row.get('fueltypeid')}",
                "period": row.get("period"),
                "state": row.get("location"),
                "fuel_type": row.get("fueltypeid"),
                "metric": "generation_mwh",
                "value": row.get("generation"),
                "unit": "MWh",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })

        print(f"  Electricity offset {offset}: {len(data)} records")

        if len(data) < 5000:
            break

        offset += 5000

    print(f"Electricity generation total: {len(results)} records fetched")
    return results


def fetch_natural_gas_prices(start: str = "2015-01", end: str = "2024-12") -> list[dict]:
    url = f"{BASE_URL}/natural-gas/pri/sum/data/"
    results = []
    offset = 0

    while True:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "value",
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": 5000
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("data", [])

        if not data:
            break

        for row in data:
            results.append({
                "source": "eia_natural_gas",
                "series_id": f"ng_{row.get('duoarea')}_{row.get('process')}",
                "period": row.get("period"),
                "state": row.get("duoarea"),
                "fuel_type": "NG",
                "metric": "price_per_mcf",
                "value": row.get("value"),
                "unit": "$/MCF",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })

        print(f"  Natural gas offset {offset}: {len(data)} records")

        if len(data) < 5000:
            break

        offset += 5000

    print(f"Natural gas prices total: {len(results)} records fetched")
    return results


def fetch_renewable_generation(start: str = "2015-01", end: str = "2024-12") -> list[dict]:
    url = f"{BASE_URL}/electricity/electric-power-operational-data/data/"
    results = []
    offset = 0

    while True:
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "generation",
            "facets[fueltypeid][]": ["SUN", "WND", "HYC"],
            "start": start,
            "end": end,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": 5000
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json().get("response", {}).get("data", [])

        if not data:
            break

        for row in data:
            results.append({
                "source": "eia_renewable",
                "series_id": f"renew_{row.get('location')}_{row.get('fueltypeid')}",
                "period": row.get("period"),
                "state": row.get("location"),
                "fuel_type": row.get("fueltypeid"),
                "metric": "generation_mwh",
                "value": row.get("generation"),
                "unit": "MWh",
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })

        print(f"  Renewable offset {offset}: {len(data)} records")

        if len(data) < 5000:
            break

        offset += 5000

    print(f"Renewable generation total: {len(results)} records fetched")
    return results


def deduplicate(records: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for r in records:
        key = (r.get("series_id"), r.get("period"))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def run():
    print("Fetching electricity generation...")
    electricity = fetch_electricity_generation()

    print("\nFetching natural gas prices...")
    gas = fetch_natural_gas_prices()

    print("\nFetching renewable generation...")
    renewables = fetch_renewable_generation()

    all_records = electricity + gas + renewables
    print(f"\nTotal before dedup: {len(all_records)}")

    unique_records = deduplicate(all_records)
    print(f"Total after dedup: {len(unique_records)}")

    output_path = f"data/raw/energy_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs("data/raw", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(unique_records, f, indent=2)
    print(f"\nSaved {len(unique_records)} records to {output_path}")


if __name__ == "__main__":
    run()