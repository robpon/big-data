from mrjob.job import MRJob
from mrjob.job import MRStep
class MRAverageCancelled(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer_init=self.reducer_init,
                reducer=self.reducer
            )
        ]

    def configure_args(self):
        super(MRAverageDelay, self).configure_args()
        self.add_file_arg("--airlines")

    def mapper(self, key, value):
        ( year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport, destination_airport,
            scheduled_departure, departure_time, departure_delay, taxi_out, wheels_off, scheduled_time, elapsed_time,
            air_time, distance, wheels_on, taxi_in, scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled,
            cancellation_reason, air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay
        ) = value.split(",")

        yield airline, int(cancelled)

    def reducer_init(self):
        self.airlines = {}
        with open("airlines.csv") as file:
            for line in file:
                data = line.split(",")
                self.airlines[data[0]] = data[1].strip()

    def reducer(self, key, values):
        sum = 0
        count = 0
        for value in values:
            sum+=value
            count+=1
        yield self.airlines[key], (sum/count)

if __name__ == "__main__":
    MRAverageCancelled.run()
