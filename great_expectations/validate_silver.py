import great_expectations as gx
from deltalake import DeltaTable
import pandas as pd

SILVER_PATH = "data/silver/energy"

def load_silver_as_df() -> pd.DataFrame:
    dt = DeltaTable(SILVER_PATH)
    df = dt.to_pandas()
    print(f"Loaded {len(df)} records from Silver Delta Lake")
    return df


def run_validation():
    df = load_silver_as_df()

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas("silver_energy_source")
    data_asset = data_source.add_dataframe_asset(name="silver_energy_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_batch")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_energy_suite"))

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="series_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="state_name")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=10_000_000)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="fuel_type_name",
            value_set=["Coal", "Natural Gas", "Nuclear", "Solar", "Wind", "Hydro"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="period",
            regex=r"^\d{4}-\d{2}$"
        )
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    results = batch.validate(suite)

    print("\n--- Silver Validation Results ---")
    print(f"Success: {results.success}")
    print(f"Total expectations: {len(results.results)}")
    passed = sum(1 for r in results.results if r.success)
    print(f"Passed: {passed}/{len(results.results)}")

    for r in results.results:
        status = "PASS" if r.success else "FAIL"
        exp_type = r.expectation_config.type
        print(f"  [{status}] {exp_type}")
        if not r.success:
            print(f"    Details: {r.result}")

    return results


if __name__ == "__main__":
    run_validation()