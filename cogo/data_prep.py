import requests
import pandas as pd
from pathlib import Path


def download_datasets(APP_ROOT):
    # Create the ./data and ./output directories if they don't exist and
    # download the COGO trip dataset
    Path(APP_ROOT / 'data').mkdir(parents=True, exist_ok=True)
    Path(APP_ROOT / 'output').mkdir(parents=True, exist_ok=True)

    if not Path(APP_ROOT / 'data' / 'cogo_trip_data.csv').is_file():
        data = requests.get(
            'https://data.smartcolumbusos.com/api/v1/dataset/' +
            '4053a9a2-fc8f-437c-af56-d104d4b5d63c/download?_format=csv')
        with open(APP_ROOT / 'data' / 'cogo_trip_data.csv', 'w') as outfile:
            outfile.write(data.text)

    if not Path(APP_ROOT / 'data' / 'cogo_stations.csv').is_file():
        data = requests.get(
            'http://opendata.columbus.gov/datasets/' +
            'a68e8a241e374eac97762c4a90db8353_0.csv')
        with open(APP_ROOT / 'data' / 'cogo_stations.csv', 'w') as outfile:
            outfile.write(data.text)


def load_datasets(APP_ROOT):
    download_datasets(APP_ROOT)

    # Column headings are a little borked from the download.
    # Not sure what's up with that, but seems reproducible
    # from the couple separate times I've downloaded the file
    # so we'll just assume that it's deterministic and kinda
    # go with that. What could go wrong?
    cogo_data = pd.read_csv(
        APP_ROOT / 'data' / 'cogo_trip_data.csv',
        header=0,
        skiprows=0,
        names=[
            'bike_id',
            'user_gender',
            'start_station_id',
            'start_station_lat',
            'start_station_long',
            'start_station_name',
            'start_timestamp',
            'stop_station_id',
            'stop_station_lat',
            'stop_station_long',
            'stop_station_name',
            'stop_timestamp',
            'user_type',
            'user_birth_year'
        ]
    )

    cogo_data['start_timestamp'] = pd.to_datetime(
        cogo_data['start_timestamp'],
        format='%m/%d/%Y %H:%M:%S',
        errors='coerce')
    cogo_data = cogo_data[cogo_data['start_timestamp'].notnull()]
    cogo_data['departure_hour'] = (
        cogo_data['start_timestamp'].dt.strftime('%H'))
    cogo_data['departure_day'] = (
        cogo_data['start_timestamp'].dt.strftime('%Y-%m-%d'))
    cogo_data['stop_timestamp'] = pd.to_datetime(
        cogo_data['stop_timestamp'],
        format='%m/%d/%Y %H:%M:%S',
        errors='coerce')
    cogo_data = cogo_data[cogo_data['stop_timestamp'].notnull()]
    cogo_data['arrival_hour'] = (
        cogo_data['stop_timestamp'].dt.strftime('%H'))
    cogo_data['arrival_day'] = (
        cogo_data['stop_timestamp'].dt.strftime('%Y-%m-%d'))
    cogo_data['trip_duration'] = (
        cogo_data['stop_timestamp'] - cogo_data['start_timestamp'])

    cogo_data['trip_id'] = cogo_data.index.values

    cogo_stations = pd.read_csv(
        APP_ROOT / 'data' / 'cogo_stations.csv'
    )

    return cogo_data, cogo_stations


def prepare_hourly_trips(cogo_data):
    hourly_trips = (
        cogo_data
        .groupby(
            ['stop_station_id', 'arrival_hour'],
            as_index=True)
        .agg({'trip_id': 'size', 'arrival_day': pd.Series.nunique})
        .rename_axis(['station_id', 'hour'])
        .rename({
            'trip_id': 'arrival_count',
            'arrival_day': 'arrival_nobs'
        }, axis=1)
    )
    hourly_trips = hourly_trips.join(
        cogo_data
        .groupby(
            ['start_station_id', 'departure_hour'],
            as_index=True)
        .agg({'trip_id': 'size', 'departure_day': pd.Series.nunique})
        .rename_axis(['station_id', 'hour'])
        .rename({
            'trip_id': 'departure_count',
            'departure_day': 'departure_nobs'
        }, axis=1)
    )

    hourly_trips['inter_arrival_time'] = 60 / (
        hourly_trips['arrival_count'] / hourly_trips['arrival_nobs'])
    hourly_trips['inter_departure_time'] = 60 / (
        hourly_trips['departure_count'] / hourly_trips['departure_nobs'])
    hourly_trips = hourly_trips.reset_index()

    return hourly_trips


def build_station_interlinks(cogo_data):
    station_crosslinks = (
        cogo_data
        .groupby(
            ['start_station_id', 'stop_station_id'],
            as_index=True)
        .agg({
            'trip_id': 'size',
            'trip_duration': pd.Series.mean
        })
        .rename({
            'trip_id': 'arrival_count',
            'trip_duration': 'average_trip_duration'},
            axis=1)
    )

    station_crosslinks = station_crosslinks.join(
        cogo_data
        .groupby(
            ['start_station_id'],
            as_index=True)
        .agg({
            'trip_id': 'size',
            'departure_day': pd.Series.nunique})
        .rename({
            'trip_id': 'total_station_departure_count',
            'departure_day': 'start_station_days_in_service'},
            axis=1)
    )

    station_crosslinks = station_crosslinks.join(
        cogo_data
        .groupby(
            ['stop_station_id'],
            as_index=True)
        .agg({'arrival_day': pd.Series.nunique})
        .rename(
            {'arrival_day': 'stop_station_days_in_service'},
            axis=1)
    )

    station_crosslinks = station_crosslinks.reset_index()
    station_crosslinks['arrival_probability'] = ((
        station_crosslinks['arrival_count'] /
        station_crosslinks['stop_station_days_in_service']) / (
        station_crosslinks['total_station_departure_count'] /
        station_crosslinks['start_station_days_in_service']
    ))

    return station_crosslinks
