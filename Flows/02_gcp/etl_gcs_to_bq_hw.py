from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3,log_prints=True)
def extract_from_gcs(color:str,year:int,month:int)-> Path:
    #download data from GCS
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    print(Path(f"../data/{gcs_path}"))
    df=pd.read_parquet(gcs_path)
    return df


@task()
def write_bq(df:pd.DataFrame, log_prints=True)-> None:
    #write data frame to bigquery
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcs-credentials")
    df.to_gbq(
        destination_table= "dezoomcamp.rides",
        project_id= "strange-vine-382204",
        credentials=gcp_credentials_block.get_credentials_from_service_account() ,
        chunksize=500_000,
        if_exists="append"
    )



@flow()
def etl_gcs_to_bq(color:str, year:int, month:int) -> None:
    #main etl flow to load data into bigqury
    df = extract_from_gcs(color,year,month)
    write_bq(df)

@flow()
def etl_parent_flow(color:str = "yellow", year:int =2019, months:list[int]=[1,2]):
   for month in months:
      etl_gcs_to_bq(color, year, month)
      
   

if __name__ == '__main__':
   color="yellow"
   year=2019
   months=[2,3]
   etl_parent_flow(color, year, months)