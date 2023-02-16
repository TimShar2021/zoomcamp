from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    try:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    except:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    df['VendorID'] =  df['VendorID'].astype('float64')            
    df['passenger_count'] =   df['passenger_count'].astype('int64')
    df['trip_distance'] = df['trip_distance'].astype('float64')         
    df['RatecodeID'] = df['RatecodeID'].astype('float64') 
    df['store_and_fwd_flag'] =  df['store_and_fwd_flag'].astype('str')
    df['PULocationID'] = df['PULocationID'].astype('int64')
    df['DOLocationID'] = df['DOLocationID'].astype('int64')             
    df['payment_type'] = df['payment_type'].astype('float64')
    df['fare_amount'] = df['fare_amount'].astype('float64')
    df['extra'] = df['extra'].astype('float64')
    df['mta_tax'] = df['mta_tax'].astype('float64')
    df['tip_amount'] = df['tip_amount'].astype('float64')
    df['tolls_amount'] = df['tolls_amount'].astype('float64')
    df['improvement_surcharge'] = df['improvement_surcharge'].astype('float64')
    df['total_amount'] = df['total_amount'].astype('float64')
    df['congestion_surcharge'] = df['congestion_surcharge'].astype('float64')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/fhv/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_main_flow(
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], year: int = 2019
):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_main_flow(months, year)
