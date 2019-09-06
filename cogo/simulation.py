import pandas as pd


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

    def __init__(self):
        self.docks = 0
        self.bikeshare_id = 0
        self.docked_bikes = []
        self.time_since_last_departure = 0

    def _check_available_docks(self):
        if self.docks < len(self.docked_bikes):
            return True
        return False

    def lease_dock(self, bike):
        if self.check_available_docks():
            self.docked_bikes.append(bike)
            return
        else:
            raise Exception(
                'No docks are available. This bike has been consigned to the void. ' +
                'Better luck next time.')

    def release_dock(self):
        if self.docked_bikes:
            # Let's be nice and use FIFO so everything cycles out a little
            # more regularly
            _undocked_bike = self.docked_bikes[0]
            self.docked_bikes.pop(0)
            return _undocked_bike
        else:
            raise Exception(
                'No more bikes are available to ride. This rider has been consigned to the void. ' +
                'Better luck next time.')

    def should_bike_depart(self, hour: int, station_interlinks: pd.DataFrame):
        return True

    def get_destination_station(self, station_interlinks: pd.DataFrame):
        return


class Bike:

    def __init__(self):
        self.bike_id = 0
        self.is_docked = True
        self.last_departure_from = 0
        self.next_arrival_to = 0
        self.remaining_transit_time = 0


