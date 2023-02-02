from prefect import flow
from prefect.filesystems import GitHub
from pathlib import Path

@flow(log_prints=True)
def etl_master_github_flow(
    months: list[int] = [11], year: int = 2020, color: str = "green"
):
    """Master ETL Flow which pulls flow code from remote GitHub
    repo and then runs the ETL process"""
    github_path = Path(f"week-2/flows/02_gcp")
    github_block = GitHub.load("github-zoomcamp-connector")
    github_block.get_directory(from_path=github_path, local_path=f"./flows-git/")

if __name__ == "__main__":
    months = [11]
    year = 2020
    color = "green"
    etl_master_github_flow(months, year, color)