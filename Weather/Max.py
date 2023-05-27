from mrjob.job import MRJob
from mrjob.job import MRStep
from datetime import datetime
class MRMax(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

    def mapper(self, _, value):
        [
            EventId, Type, Severity, StartTime, EndTime,
            Precipitation, TimeZone, AirportCode, LocationLat, LocationLng, City, County, State, ZipCode
        ] = value.split(",")

        StartTime = datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
        EndTime = datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")
        r = (EndTime - StartTime).seconds
        yield Type , (int(r), City)

    def reducer(self,  key, values):
        type = {}
        for value in values:
            try:
                type[value[1]]+= value[0]
            except:
                maxKey = value[1]
                type[value[1]] = value[0]
        for key_1 in type:
            if type[key_1] > type[maxKey]:
                maxKey = key_1
        yield key, (maxKey, type[maxKey])


if __name__ == "__main__":
    MRMax.run()