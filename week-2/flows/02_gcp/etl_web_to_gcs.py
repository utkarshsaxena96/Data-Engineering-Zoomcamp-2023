from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3, log_prints=True)
def extract_data(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from the URL and return a DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix data type issues in the DataFrame"""
    if color == 'green':
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif color == 'yellow':
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    
    print(df.head(2))
    print(f'columns data types: {df.dtypes}')
    print(f'row count: {len(df)}')
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Convert the dataframe and write to local filesystem in Parquet format"""
    local_path = Path(f"data/{color}/{dataset_file}.parquet")
    print(f"saving the dataframe at path: {local_path}")
    df.to_parquet(local_path, compression='gzip')
    return local_path

@task(log_prints=True)
def load_to_gcs(path: Path) -> None:
    """Upload local parquet file to GCS Bucket"""
    gcs_block = GcsBucket.load("gcs-connector-block")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return True

@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"
    df = extract_data(dataset_url)
    clean_df = clean_data(df, color)
    local_path = write_local(clean_df, color, dataset_file)
    load_to_gcs(local_path)

@flow(log_prints=True)
def etl_parent_web_to_gcs_flow(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    months = [2,3]
    year = 2019
    color = "yellow"
    etl_parent_web_to_gcs_flow(months, year, color)


