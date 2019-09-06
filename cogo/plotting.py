# Code in this file unless otherwise noted is copied from examples
# built by Uber to demonstrate H3 Python bindings. The original
# source is available at
# https://nbviewer.jupyter.org/github/uber/h3-py-notebooks
import json
import branca.colormap as cm
from branca.colormap import linear

from h3 import h3
from geojson.feature import *

import folium
from folium import Map, Marker, GeoJson
from folium.plugins import MarkerCluster


def counts_by_hexagon(df, resolution):
    '''Use h3.geo_to_h3 to index each data point into the spatial index
    of the specified resolution.

    Use h3.h3_to_geo_boundary to obtain the geometries of these hexagons'''

    df = df[[
        'start_station_lat', 'start_station_long',
        'start_station_id', 'stop_station_id',
        'date']]
    stations = (
        df[['start_station_lat', 'start_station_long',
            'start_station_id']]
        .drop_duplicates()
        .rename(
            columns=[{
                'start_station_lat': 'station_lat',
                'start_station_long': 'station_long',
                'start_station_id': 'station_id'
            ]}
        )
    )
    stations['hex_id'] = stations.apply(
        lambda row: h3.geo_to_h3(
            row['start_station_lat'], row['start_station_long'],
            resolution),
        axis=1)

    df_agg = (
        pd.DataFrame(
            data=df.groupby(
                by=['start_station_id', 'date'],
                as_index=False
            )
            .size()
        )
        .rename_axis(index={'start_station_id': 'station_id'})
        .rename(columns={0: 'departure_count'})
    )

    df_agg = df_agg.join(
        pd.DataFrame(
            data=df.groupby(
                by=['stop_station_id', 'date'],
                as_index=False
            )
            .size()
        )
        .rename_axis(index={'stop_station_id': 'station_id'})
        .rename(columns={0: 'arrival_count'})
    )
    df_agg = pd.merge(df_agg, stations)

    df_agg['geometry'] = (
        df_agg
        .hex_id
        .apply(
            lambda x: {
                'type': 'Polygon',
                'coordinates': [
                    h3.h3_to_geo_boundary(h3_address=x, geo_json=True)]
            }
        ))

    return df_agg


def hexagons_dataframe_to_geojson(df_hex, value_col, file_output=None):
    '''Produce the GeoJSON for a dataframe that has a geometry
    column in geojson format already, along with the columns
    hex_id and value '''
    list_features = []

    for idx, row in df_hex.iterrows():
        feature = Feature(
            geometry=row['geometry'],
            id=row['hex_id'],
            properties={'value': row[value_col]})
        list_features.append(feature)

    feat_collection = FeatureCollection(list_features)
    geojson_result = json.dumps(feat_collection)

    if file_output is not None:
        with open(file_output, 'w') as f:
            json.dump(feat_collection, f)

    return geojson_result


def choropleth_map(df_agg, value_col, border_color='black', fill_opacity=0.7,
                   initial_map=None, with_legend=False, kind='linear'):
    min_value = df_agg[value_col].min()
    max_value = df_agg[value_col].max()
    m = round((min_value + max_value) / 2, 0)
    res = h3.h3_get_resolution(df_agg.loc[0, 'hex_id'])

    if initial_map is None:
        initial_map = Map(
            location=[39.970208, -83.000645],
            zoom_start=13,
            tiles='cartodbpositron',
            attr=(
                '© <a href="http://www.openstreetmap.org/copyright">' +
                'OpenStreetMap</a> contributors © <a href="http://cartodb.' +
                'com/attributions#basemaps">CartoDB</a>'
            )
        )

    if kind == 'linear':
        custom_cm = cm.LinearColormap(
            ['green', 'yellow', 'red'],
            vmin=min_value,
            vmax=max_value)
    elif kind == 'outlier':
        custom_cm = cm.LinearColormap(
            ['blue', 'white', 'red'],
            vmin=min_value,
            vmax=max_value)
    elif kind == 'filled_nulls':
        custom_cm = cm.LinearColormap(
            ['sienna', 'green', 'yellow', 'red'],
            index=[0, min_value, m, max_value],
            vmin=min_value,
            vmax=max_value)

    geojson_data = hexagons_dataframe_to_geojson(
        df_hex=df_agg, value_col=value_col)
    name_layer = 'Choropleth ' + str(res)
    if kind != 'linear':
        name_layer = name_layer + kind

    GeoJson(
        geojson_data,
        style_function=lambda feature: {
            'fillColor': custom_cm(feature['properties']['value']),
            'color': border_color,
            'weight': 1,
            'fillOpacity': fill_opacity
        },
        name=name_layer
    ).add_to(initial_map)

    if with_legend is True:
        custom_cm.add_to(initial_map)

    return initial_map
