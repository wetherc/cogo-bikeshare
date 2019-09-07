import math
import pandas as pd
import numpy as np


# Initialize stations with N of M bikes
#
# For each tick:
#   For each station:
#     - determine if a departure should occur (sample from geometric
#       distribution given that station's inter-departure interval
#       for that time of day)
#     If departure:
#       If n > 0 bikes available:
#         - determine destination station
#         - push bike to global 'in transit' list w/departure time
#         - pop bike from station (free a dock)
#         - reset station's time since last depart counter
#       Else:
#         - panic
#     Else:
#       - increment station's time since last depart counter
#
#   For each undocked bike:
#     - Decrement remaining travel time by 1 tick
#     If remaining travel time == 0:
#       If n > 0 available docks at destination station:
#         - pop bike from in transit list
#         - push bike to station (lease a dock)
#       Else:
#         - panic
class Station:

    def __init__(self, bikeshare_id: int, docks: int):
        self.bikeshare_id = bikeshare_id
        self.docks = docks
        self.docked_bikes = []
        self.time_since_last_departure = 0
        self.time_until_next_departure = 0
        self.interlinks = {
            'probability': {},
            'travel_time': {}
        }
        self.inter_departure_times = dict()

    def _check_available_docks(self):
        if self.docks > len(self.docked_bikes):
            return True
        return False

    def lease_dock(self, bike):
        if self._check_available_docks():
            bike.is_docked = True
            self.docked_bikes.append(bike)
            return True
        else:
            return False

    def release_dock(self, destination, transit_time):
        if self.docked_bikes:
            # Let's be nice and use FIFO so everything cycles out a little
            # more regularly
            _undocked_bike = self.docked_bikes[0]
            _undocked_bike.is_docked = False
            _undocked_bike.last_departure_from = self.bikeshare_id
            _undocked_bike.next_arrival_to = destination
            _undocked_bike.remaining_transit_time = int(
                transit_time.total_seconds())

            self.docked_bikes.pop(0)
            return _undocked_bike
        else:
            return None

    def should_bike_depart(self, hour: str):
        if self.time_until_next_departure == 0:
            # This will return the number of trials needed until
            # a success event occurs, sampled from a geometric
            # distribution, with probability, p, of an individual
            # success given by 1 / inter_departure_time
            _inter_departure_interval = self.inter_departure_times.get(hour)
            if not _inter_departure_interval:
                _inter_departure_interval = np.mean([
                    val for val in self.inter_departure_times.values()
                ])
            self.time_until_next_departure = np.random.geometric(
                p=1/_inter_departure_interval)
            self.time_since_last_departure = 0

            return True
        self.time_until_next_departure -= 1
        self.time_since_last_departure += 1
        return False

    def get_destination_station(self):
        station = np.random.choice(
            self.interlinks['station'],
            size=1,
            replace=False,
            p=self.interlinks['probability']
        )[0]
        _idx = self.interlinks['station'].index(station)
        return (station, self.interlinks['travel_time'][_idx])


class Bike:

    def __init__(self, id: int):
        self.bike_id = 0
        self.is_docked = True
        self.last_departure_from = 0
        self.next_arrival_to = 0
        self.remaining_transit_time = 0


class Orchestrator:

    def __init__(self, cogo_stations: pd.DataFrame,
                 station_crosslinks: pd.DataFrame,
                 hourly_trips: pd.DataFrame, bike_count: int):

        station_crosslinks = station_crosslinks.loc[
            station_crosslinks['stop_station_id'].isin(
                cogo_stations['BIKESHARE_ID']
            )
        ]
        self.stations = self._instantiate_stations(
            cogo_stations,
            station_crosslinks,
            hourly_trips)
        self.bikes_in_transit = []
        self._distribute_bikes(bike_count)

    def _instantiate_stations(self, _cogo_stations, _station_crosslinks,
                              _hourly_trips):
        stations = dict()

        for idx, row in _cogo_stations.iterrows():
            _id = row['BIKESHARE_ID']
            _docks = row['DOCKS']
            station = Station(_id, _docks)
            station.interlinks = {
                'station': _station_crosslinks.loc[
                        _station_crosslinks['start_station_id'] == _id,
                        'stop_station_id'
                    ].tolist(),
                'probability': _station_crosslinks.loc[
                        _station_crosslinks['start_station_id'] == _id,
                        'arrival_probability'
                    ].tolist(),
                'travel_time': _station_crosslinks.loc[
                        _station_crosslinks['start_station_id'] == _id,
                        'average_trip_duration'
                    ].tolist()
            }

            if not station.interlinks['station']:
                # Our station data include some more recent entries than
                # our trip data, so we should probably make some minimal
                # effort to keep everything from catching fire when we
                # encounter them...
                continue

            station.interlinks['probability'] = [
                float(prob) / sum(station.interlinks['probability'])
                for prob in station.interlinks['probability']
            ]
            station.inter_departure_times = dict(
                (row['hour'], row['inter_departure_time'])
                for _, row in _hourly_trips.loc[
                    _hourly_trips['station_id'] == _id].iterrows()
            )

            stations[_id] = station
        return stations

    def _distribute_bikes(self, _bike_count):
        _stationlist = list(self.stations.keys())
        _counter = 0
        for idx in range(_bike_count):
            _bike = Bike(idx)
            _lastval = _counter

            while (
                len(self.stations[_stationlist[_counter]].docked_bikes) /
                self.stations[_stationlist[_counter]].docks
            ) > 0.75:
                if _counter == (len(_stationlist) - 1):
                    _counter = 0
                else:
                    _counter += 1
                if _counter == _lastval:
                    raise Exception(
                        'You have tried to provision too many bikes for the ' +
                        'number of docks available. Please reduce the ' +
                        'number of bikes, or increase the available docks.')

            self.stations[_stationlist[_counter]].lease_dock(_bike)
            if _counter == (len(_stationlist) - 1):
                _counter = 0
            else:
                _counter += 1
        return

    def run_simulation(self, start_hour: int, num_ticks: int):
        tick = 0
        while tick < num_ticks:
            _current_hour = start_hour + (num_ticks // 60)
            if _current_hour < 10:
                _current_hour = '0' + str(_current_hour)
            else:
                _current_hour = str(_current_hour)

            for station in self.stations.values():
                if station.should_bike_depart(_current_hour):
                    _dest, _travel_time = station.get_destination_station()
                    _lease = station.release_dock(_dest, _travel_time)
                    if _lease:
                        self.bikes_in_transit.append(_lease)
                    else:
                        print(
                            f'No bike was available from station '
                            f'{station.bikeshare_id} at hour {_current_hour}. '
                            'The customer was sad.')

            for bike in self.bikes_in_transit:
                bike.remaining_transit_time -= 1
                if bike.remaining_transit_time <= 0:
                    _lease = (
                        self.stations[bike.next_arrival_to].lease_dock(bike))
                    if _lease is True:
                        self.bikes_in_transit.pop(
                            self.bikes_in_transit.index(bike))
                    else:
                        print(
                            'No docks available at station '
                            f'{bike.next_arrival_to}... Waiting for one'
                            'to become available.')

            tick += 1
