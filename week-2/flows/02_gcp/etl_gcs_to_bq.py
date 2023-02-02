import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Extract data from GCS Bucket and store in local"""
    gcs_path = Path(f"data/{color}/{color}_tripdata_{year}-{month:02}.csv.gz.parquet")
    gcs_block = GcsBucket.load("gcs-connector-block")
    gcs_block.get_directory(from_path=gcs_path, local_path="./data/")
    return Path(f"./data/{gcs_path}")

@task(retries=3, log_prints=True)
def transform_data(path: Path) -> pd.DataFrame:
    """Clean the data and return a DataFrame"""
    df = pd.read_parquet(path)
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(df.head(2))
    print(f"columns data types: {df.dtypes}")
    print(f"Row count: {len(df)}")
    return df

@task(retries=3, log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write the DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-credentials-block")
    df.to_gbq(
        destination_table="ny_taxi_trips.taxi_rides",
        project_id="dtc-de-zoomcamp-utksxn96",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    """Main ETL function to load data into BigQuery"""
    local_path = extract_from_gcs(color, year, month)
    df = transform_data(local_path)
    write_to_bq(df)

@flow(log_prints=True)
def etl_parent_gcs_to_bq_flow(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
    months = [2,3]
    year = 2019
    color = "yellow"
    etl_parent_gcs_to_bq_flow(months, year, color)
