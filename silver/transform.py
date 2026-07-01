import json
import os
from datetime import datetime, timezone
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pyarrow.compute as pc
from groq import Groq
from dotenv import load_dotenv
import time

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))


BRONZE_PATH = "data/bronze/energy"
SILVER_PATH = "data/silver/energy"


FUEL_TYPE_MAP = {
    "COL": "Coal",
    "NG": "Natural Gas",
    "NUC": "Nuclear",
    "SUN": "Solar",
    "WND": "Wind",
    "HYC": "Hydro"
}

STATE_MAP = {
    "CA": "California", "TX": "Texas", "NY": "New York",
    "FL": "Florida", "WA": "Washington", "CO": "Colorado",
    "IL": "Illinois", "PA": "Pennsylvania", "OH": "Ohio",
    "GA": "Georgia"
}

def read_bronze() -> pa.Table:
    dt = DeltaTable(BRONZE_PATH)
    table = dt.to_pyarrow_table()
    print(f"Read {table.num_rows} records from Bronze Delta Lake")
    return table


def clean(table: pa.Table) -> pa.Table:
    df = table.to_pydict()

    cleaned = []
    skipped = 0

    for i in range(len(df["series_id"])):
        value = df["value"][i]
        period = df["period"][i]
        state = df["state"][i]
        source = df["source"][i]
        fuel_type = df["fuel_type"][i]

        # drop nulls on critical fields
        if value is None or period is None or state is None:
            skipped += 1
            continue

        # drop negative values
        if value < 0:
            skipped += 1
            continue

        # normalize fuel type label
        fuel_label = FUEL_TYPE_MAP.get(fuel_type, fuel_type)

        # normalize state label
        state_label = STATE_MAP.get(state, state)

        cleaned.append({
            "series_id": df["series_id"][i],
            "source": source,
            "period": period,
            "state_code": state,
            "state_name": state_label,
            "fuel_type_code": fuel_type,
            "fuel_type_name": fuel_label,
            "metric": df["metric"][i],
            "value": float(value),
            "unit": df["unit"][i],
            "ingested_at": df["ingested_at"][i],
            "transformed_at": datetime.now(timezone.utc).isoformat()
        })

    print(f"Cleaned: {len(cleaned)} records kept, {skipped} dropped")
    return cleaned


def generate_llm_summary(state: str, fuel: str, period: str, value: float, unit: str, metric: str) -> str:
    prompt = f"""You are a data analyst summarizing US energy data.

Given this single data point:
- State: {state}
- Fuel type: {fuel}
- Period: {period}
- Metric: {metric}
- Value: {value:,.2f} {unit}

Write a single plain-English sentence summarizing this data point in business context.
Be concise. No more than 30 words. Do not use bullet points."""

    response = groq_client.chat.completions.create(
    model="openai/gpt-oss-20b",
    messages=[{"role": "user", "content": prompt}],
    max_tokens=100
    )
    return response.choices[0].message.content.strip()


def enrich_with_llm(cleaned: list[dict], sample_size: int = 20) -> list[dict]:
    """Run LLM summaries on a sample -- full run would burn API credits"""
    print(f"Running LLM enrichment on {sample_size} sampled records...")

    step = max(1, len(cleaned) // sample_size)
    sampled_indices = set(range(0, len(cleaned), step)[:sample_size])

    for i, record in enumerate(cleaned):
        if i in sampled_indices:
            try:
                summary = generate_llm_summary(
                    state=record["state_name"],
                    fuel=record["fuel_type_name"],
                    period=record["period"],
                    value=record["value"],
                    unit=record["unit"],
                    metric=record["metric"]
                )
                record["llm_summary"] = summary
                print(f"  Enriched record {i}: {summary[:60]}...")
                time.sleep(4)  # stay under free tier rate limit
            except Exception as e:
                record["llm_summary"] = None
                print(f"  LLM error on record {i}: {e}")
        else:
            record["llm_summary"] = None

    enriched = sum(1 for r in cleaned if r["llm_summary"] is not None)
    print(f"LLM enrichment done: {enriched} records enriched")
    return cleaned


def write_silver(records: list[dict]):
    schema = pa.schema([
        pa.field("series_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("period", pa.string()),
        pa.field("state_code", pa.string()),
        pa.field("state_name", pa.string()),
        pa.field("fuel_type_code", pa.string()),
        pa.field("fuel_type_name", pa.string()),
        pa.field("metric", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string()),
        pa.field("ingested_at", pa.string()),
        pa.field("transformed_at", pa.string()),
        pa.field("llm_summary", pa.string()),
    ])

    table = pa.Table.from_pylist(records, schema=schema)
    os.makedirs(SILVER_PATH, exist_ok=True)
    write_deltalake(
        SILVER_PATH,
        table,
        mode="overwrite",
        partition_by=["source", "period"]
    )
    print(f"Written {len(records)} records to Silver Delta Lake at {SILVER_PATH}")


def run():
    print("Reading Bronze...")
    bronze = read_bronze()

    print("\nCleaning...")
    cleaned = clean(bronze)

    print("\nLLM Enrichment...")
    enriched = enrich_with_llm(cleaned, sample_size=50)

    print("\nWriting Silver...")
    write_silver(enriched)

    print("\nSilver layer complete.")


if __name__ == "__main__":
    run()