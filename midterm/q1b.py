class q1b(MockMR):
    def mapper_init(self):
        self.airport_cache ={}

    def mapper(self, key, line):
        limit = 20
        rows =line.split(",")
        airline,src,dest,stops=rows
        if rows != ['Airline','Source','Destination','Stops']:
            if dest in self.airport:
                self.airport_cache[dest] += 1
            else:
                self.airport_cache =1

    def mapper_final(self):
        for dest in self.airport_cache:
            yield (dest, self.airport_cache[dest])


    def reducer(self, key, values):
        #keys: source airport, value: destination airport
        yield key, str(values)


if __name__ == "__main__":

    q1b.run(trace=True)
