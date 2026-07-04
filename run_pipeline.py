import subprocess
import sys
import os

def run(command, description):
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"{'='*60}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"\nFAILED: {description}")
        sys.exit(1)
    print(f"DONE: {description}")

def main():
    # make sure we're in project root
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    run("python ingestion/fetch_energy.py", "Fetching EIA data from API")
    run("python kafka/producer.py", "Publishing records to Kafka")
    run("python kafka/consumer.py", "Consuming from Kafka to Bronze Delta Lake")
    run("python silver/transform.py", "Cleaning + LLM enrichment to Silver Delta Lake")
    run("python snowflake/load_silver.py", "Loading Silver to Snowflake")
    run("cd energy_dbt && dbt run", "Building dbt Gold models")
    run("cd energy_dbt && dbt test", "Running dbt tests")
    run("cd .. && python great_expectations/validate_bronze.py", "Validating Bronze layer")
    run("python great_expectations/validate_silver.py", "Validating Silver layer")

    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE -- all steps passed successfully")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()