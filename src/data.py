from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from src.paths import RAW_DATA_DIR, PROCESSED_DATA_DIR

def download_file(year: int, month: int) -> Path:
    """"""
    URL = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    response = requests.get(URL)

    if response.status_code == 200:
        path = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'
        open(path, 'wb').write(response.content)
        return path
    else:
        raise Exception(f'Failed to download file from {URL} or it\'s not available')
    
def validate_raw_data(
        rides_df: pd.DataFrame,
        year: int,
        month: int,
) -> pd.DataFrame:
    this_month_start = f'{year}-{month:02d}-01'
    next_month_start = f'{year}-{month+1:02d}-01' if month < 12 else f'{year+1}-01-01'
    rides_df = rides_df[rides_df.pickup_datetime >= this_month_start]
    rides_df = rides_df[rides_df.pickup_datetime < next_month_start]
    return rides_df

def load_raw_data(
        year: int,
        months: Optional[List[int]] = None,
) -> pd.DataFrame:
    rides_df = pd.DataFrame()
    if months is None:
        months = list(range(1, 13))
    elif isinstance(months, int):
        months = [months]
    for month in months:
        local_file = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'
        if not local_file.exists():
            try:
                download_file(year, month)
            except:
                print(f'Failed to download file for {year}-{month:02d}')
                
        else:
            print(f'File for {year}-{month:02d} already exists')
        
        rides_one_month = pd.read_parquet(local_file)
        rides_one_month = rides_one_month[['tpep_pickup_datetime', 'PULocationID']]
        rides_one_month.rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'PULocationID': 'pickup_location_id'}, inplace=True)
        rides_one_month = validate_raw_data(rides_one_month, year, month)
        rides_df = pd.concat([rides_df, rides_one_month])
    
    rides_df = rides_df[['pickup_datetime', 'pickup_location_id']]
    return rides_df

def add_missing(aggregated_rides_df: pd.DataFrame) -> pd.DataFrame:
    location_ids = aggregated_rides_df['pickup_location_id'].unique()
    full_range = pd.date_range(aggregated_rides_df['pickup_hour'].min(), aggregated_rides_df['pickup_hour'].max(), freq='H')
    output = pd.DataFrame()

    for location_id in tqdm(location_ids):
        # select rides for a particular location
        aggregated_rides_iter = aggregated_rides_df.loc[aggregated_rides_df.pickup_location_id == location_id, ['pickup_hour', 'ride_count']]

        # adding missing dates with 0 count
        aggregated_rides_iter.set_index('pickup_hour', inplace=True)
        aggregated_rides_iter.index = pd.DatetimeIndex(aggregated_rides_iter.index)
        aggregated_rides_iter = aggregated_rides_iter.reindex(full_range, fill_value=0)

        # add location_id cols
        aggregated_rides_iter['pickup_location_id'] = location_id
        output = pd.concat([output, aggregated_rides_iter])

    # set ride day to be a column instead of index
    output = output.reset_index().rename(columns={'index': 'pickup_hour'})
    return output

