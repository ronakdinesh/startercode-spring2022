
from mockmr import MockMR
import random

class q2(MockMR):
    def mapper_init(self): 
     self.airport_cache ={}

    def mapper(self, key, line):
        rows = line.split(",")   
        airline,src,dest,stops =rows
        if rows != ['Airline','Source','Destination','Stops']:
            if src in self.airport_cache:
                self.airport_cache[src] += 1
            else:
                self.airport_cache = 1
        if rows != ['Airline','Source','Destination','Stops']:
            if dest in self.airport:
                self.airport_cache[dest] += 1
            else:
                self.airport_cache =1
            
        
    def mapper_final(self):
        for src in self.airport_cache:
            yield (src, (self.airport_cache[src],self.airport_cache[dest)


    def reducer(self, key, values):
        #keys: source airport, value: destination airport
        yield key, sum(values)


if __name__ == "__main__":

    q2.run(trace=True)