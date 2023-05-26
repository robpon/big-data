from mrjob.job import MRJob
from mrjob.job import MRStep
class MRAverageDelay(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper, reducer_init = self.reducer_init, reducer = self.reducer)
        ]

    def mapper(self, key, value):
        ( year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport, destination_airport,
            scheduled_departure, departure_time, departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time,
            air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled,
            cancellation_reason, air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay
        ) = value.split(",")

        if departure_delay == '':
            departure_delay = 0
        else:
            departure_delay = float(departure_delay)

        if arrival_delay == '':
            arrival_delay = 0
        else:
            arrival_delay = float(arrival_delay)

        yield airline, (departure_delay, arrival_delay)

    def reducer(self, key, values):
        average_arrival_delay=0
        average_departure_delay=0
        count = 0
        for val in values:
            count+=1
            average_departure_delay+=val[0]
            average_arrival_delay+=val[1]
        yield key, (average_departure_delay/count, average_arrival_delay/count)

if __name__ == "__main__":
    MRAverageDelay.run()