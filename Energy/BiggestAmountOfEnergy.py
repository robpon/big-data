from mrjob.job import MRJob
from mrjob.job import MRStep

class MRBiggestAmountOfEnergy(MRJob):



    def mapper(self, _, line):
        (id, date, amount) = line.split(",")

        amount = float(amount.strip())

        yield id, amount

    def combiner(self, key, values):
        yield None, (sum(values), key)

    def reducer(self, key, value):
        max = sorted(value, reverse=True)

        yield max[0][1], max[0][0]

if __name__ == "__main__":
    MRBiggestAmountOfEnergy.run()


