from mockmr import MockMR
import random

class q1a(MockMR):
    def mapper_init(self): 
     self.airport_cache ={}

    def mapper(self, key, line):
        limit = 20
        rows = line.split(",")   
        airline,src,dest,stops =rows
        if rows != ['Airline','Source','Destination','Stops']:
            if src in self.airport_cache:
                self.airport_cache[src] += 1
            else:
                self.airport_cache = 1
            
        if len(self.airport_cache[src]) > limit:
            for i in self.airport_cache:
                yield (i, self.airport_cache[i])
            self.cache.clear()

    def mapper_final(self):
        if len(self.airport_cache[src]) != 0:
            for i in self.airport_cache:
                yield (i, self.airport_cache[i])


    def reducer(self, key, values):
        #keys: source airport, value: destination airport
        yield key, sum(values)


if __name__ == "__main__":

    q1a.run(trace=True)