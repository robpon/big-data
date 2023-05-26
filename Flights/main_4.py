from mrjob.job import MRJob
from mrjob.job import MRStep
class MRAverageDelay(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, key, value):
        ( year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport, destination_airport,
            scheduled_departure, departure_time, departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time,
            air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled,
            cancellation_reason, air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay
        ) = value.split(",")

        yield day_of_week, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    MRAverageDelay.run()