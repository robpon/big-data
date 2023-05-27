from mrjob.job import MRJob
from mrjob.job import MRStep
from datetime import datetime
class MRBasicStats(MRJob):



    def mapper(self, _, value):
        [
            EventId, Type, Severity, StartTime, EndTime,
            Precipitation, TimeZone, AirportCode, LocationLat, LocationLng, City, County, State, ZipCode
        ] = value.split(",")

        StartTime = datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
        EndTime = datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")
        r = (EndTime - StartTime).seconds
        yield (City, Type, StartTime.year) ,int(r)

    def reducer(self, key, values):
        ySconds = 365*24*60*60
        rSeconds = 0
        for value in values:
            rSeconds+=value
        part = rSeconds/ySconds
        yield key, (part, rSeconds, ySconds)



if __name__ == "__main__":
    MRBasicStats.run()