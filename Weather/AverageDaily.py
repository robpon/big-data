from mrjob.job import MRJob
from mrjob.job import MRStep
from datetime import datetime
class MRMax(MRJob):


    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.final_reducer
            )
        ]

    def mapper(self, _, value):
        [
            EventId, Type, Severity, StartTime, EndTime,
            Precipitation, TimeZone, AirportCode, LocationLat, LocationLng, City, County, State, ZipCode
        ] = value.split(",")

        StartTime = datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
        EndTime = datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")

        StartDate = str(StartTime.month).zfill(2)+"-"+str(StartTime.day).zfill(2)
        EndDate = str(EndTime.month).zfill(2)+"-"+str(EndTime.day).zfill(2)
        if StartTime.day != EndTime.day:
            DayEnd = datetime.strptime((str(EndTime.year)+"-"+EndDate),"%Y-%m-%d")
            yield (City, StartDate), (Type, (DayEnd-StartTime).seconds)
            yield (City,EndDate), (Type, (EndTime-DayEnd).seconds)
        else:
            yield (City, StartDate), (Type, int((EndTime-StartTime).seconds))

    def reducer(self, key, values):
        sec = 24*60*60
        dict = {}
        for val in values:
            try:
                dict[val[0]] += val[1]
            except:
                dict[val[0]] = val[1]

        for key_1 in dict:
            dict[key_1] = round(((dict[key_1]/sec)*100), 1)
        yield None, (key, dict)


    def final_reducer(self, _, value):
        value = sorted(value)
        for line in value:

            yield line[0],line[1]





if __name__ == "__main__":
    MRMax.run()