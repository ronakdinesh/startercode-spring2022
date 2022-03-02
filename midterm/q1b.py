from mockmr import MockMR
import random

class q1b(MockMR):
    def mapper_init(self): 
    self.cache ={}

    def mapper(self, key, value):
        limit = 20
        parts = list(([line.strip()]))[0]   
        if parts != [Airline,Source,Destination,Stops]:
            yield parts[2],parts[1]

        if len(self.cache) > limit:
            for i in self.cache:
                yield (i, self.cache[i])
            self.cache.clear()

    def mapper_final(self):
        if len(self.cache) != 0:
            for i in self.cache:
                yield (i, self.cache[i])


    def reducer(self, key, values):
        #keys: source airport, value: destination airport
        yield key, sum(values)


if __name__ == "__main__":

    q1b.run(trace=True)