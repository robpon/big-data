from mrjob.job import MRJob
from mrjob.job import MRStep

class MRAverageAmountOfEnergy(MRJob):
    def mapper(self, _, line):
        (id, date, amount) = line.split(",")

        amount = float(amount.strip())

        yield id, amount



    def reducer(self, key, values):
        count = 0
        sum = 0
        for value in values:
            sum+=value
            count+=1
        average = round(sum/count, 3)
        yield key, average

if __name__ == "__main__":
    MRAverageAmountOfEnergy.run()