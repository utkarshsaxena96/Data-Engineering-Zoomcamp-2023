from prefect import flow
from etl_gcs_to_bq import *
from etl_web_to_gcs import *

@flow(retries=3, log_prints=True, )
def etl_master_flow(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    """Master ETL flow to trigger the subflows"""
    etl_parent_web_to_gcs_flow(months, year, color)
    etl_parent_gcs_to_bq_flow(months, year, color)

if __name__ == "__main__":
    months = [2,3]
    year = 2019
    color = "yellow"
    etl_master_flow(months, year, color)