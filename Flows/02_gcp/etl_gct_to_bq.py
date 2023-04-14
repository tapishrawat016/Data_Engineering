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
    return Path(f"./{gcs_path}")


@task()
def transform(path:Path)-> pd.DataFrame:
    #data clean
    df=pd.read_parquet(path)
    print(f"pre: missing passenger count : {df['passenger_count'].isna().sum()}")
    print(f"pre: missing passenger count : {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0)
    print(f"post: missing passenger count : {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df:pd.DataFrame)-> None:
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
def etl_gcs_to_bq():
    #main etl flow to load data into bigqury
    color="yellow"
    year=2021
    month=1
    path = extract_from_gcs(color,year,month)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
   etl_gcs_to_bq()