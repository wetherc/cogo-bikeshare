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
            return
        else:
            raise Exception(
                'No docks are available. This bike has been consigned ' +
                'to the void. Better luck next time.')

    def release_dock(self, destination, transit_time):
        if self.docked_bikes:
            # Let's be nice and use FIFO so everything cycles out a little
            # more regularly
            _undocked_bike = self.docked_bikes[0]
            _undocked_bike.is_docked = False
            _undocked_bike.last_departure_from = self.bikeshare_id
            _undocked_bike.next_arrival_to = destination
            _undocked_bike.remaining_transit_time = transit_time

            self.docked_bikes.pop(0)
            return _undocked_bike
        else:
            raise Exception(
                'No more bikes are available to ride. This rider has ' +
                'been consigned to the void. Better luck next time.')

    def should_bike_depart(self, hour: str):
        if self.time_until_next_departure == 0:
            # This will return the number of trials needed until
            # a success event occurs, sampled from a geometric
            # distribution, with probability, p, of an individual
            # success given by 1 / inter_departure_time
            self.time_until_next_departure = np.random.geometric(
                p=1/self.inter_departure_times[hour])
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
        self.stations = dict()
        for idx, row in cogo_stations.iterrows():
            _id = row['BIKESHARE_ID']
            _docks = row['DOCKS']
            station = Station(_id, _docks)
            station.interlinks = {
                'station': station_crosslinks.loc[
                        station_crosslinks['start_station_id'] == _id,
                        'stop_station_id'
                    ].tolist(),
                'probability': station_crosslinks.loc[
                        station_crosslinks['start_station_id'] == _id,
                        'arrival_probability'
                    ].tolist(),
                'travel_time': station_crosslinks.loc[
                        station_crosslinks['start_station_id'] == _id,
                        'average_trip_duration'
                    ].tolist()
            }
            station.interlinks['probability'] = [
                float(prob) / sum(station.interlinks['probability'])
                for prob in station.interlinks['probability']
            ]
            station.inter_departure_times = dict(
                (row['hour'], row['inter_departure_time'])
                for _, row in hourly_trips.loc[
                    hourly_trips['station_id'] == _id].iterrows()
            )

            self.stations[_id] = station

        _stationlist = list(self.stations.keys())
        _counter = 0
        for idx in range(bike_count):
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
