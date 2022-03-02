from mockmr import MockMR
import random

class q1(MockMR):
    def mapper_init(self): 
     self.airport_cache ={}

    def mapper(self, key, line):
        limit = 20
        rows = line.split(",")   
        airline,src,dest,stops =rows
        if parts != ['Airline','Source','Destination','Stops']:
            if src in self.airport_cache:
                self.airport_cache[src] += 1
            else:
                self.airport_cache = 1
            

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

    q1.run(trace=True)