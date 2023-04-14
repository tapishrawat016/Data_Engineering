from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task()
def fetch(dataset_url: str)-> pd.DataFrame:
   #read taxi data from web into pandas dataframe
   df=pd.read_csv(dataset_url)
   return df

@task(log_prints=True)
def clean(df: pd.DataFrame)-> pd.DataFrame:
   #fix some datatype issue
   df['lpep_pickup_datetime']=pd.to_datetime(df["lpep_pickup_datetime"])
   df['lpep_dropoff_datetime']=pd.to_datetime(df["lpep_dropoff_datetime"])
   print(df.head(2))
   print(f"coluns : {df.dtypes}")
   print(f"rows: {len(df)}")
   return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str)-> Path:
   #write dataframe out as parquet file"
   path=Path(f"data/{color}/{dataset_file}.parquet")
   df.to_parquet(path, compression="gzip")
   return path

@task()
def write_gcs(path:Path)-> None:
  #upload parquet file to GCS
  gcs_block= GcsBucket.load("zoomcamp-gcs")
  gcs_block.upload_from_path(
     from_path=f"{path}",
     to_path=path)
  return



@flow()
def etl_web_to_gcs()-> None:

#the main etl function
   color="green"
   year=2020
   month=11
   dataset_file=f"{color}_tripdata_{year}-{month:02}"
   dataset_url=f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

   df=fetch(dataset_url)
   df_clean=clean(df)
   path=write_local(df_clean, color, dataset_file)
   write_gcs(path)

if __name__ == '__main__':
   etl_web_to_gcs()
