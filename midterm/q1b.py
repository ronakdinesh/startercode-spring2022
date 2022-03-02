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

        if len(self.airport_cache) > limit:
            for i in self.airport_cache:
                yield (i, self.airport_cache[i])
            self.airport_cache.clear()

    def mapper_final(self):
        if len(self.cache) != 0:
            for i in self.airport_cache:
                yield (i, self.airport_cache[i])


    def reducer(self, key, values):
        #keys: source airport, value: destination airport
        yield key, str(values)


if __name__ == "__main__":

    q1b.run(trace=True)
