
from mockmr import MockMR
import csv

class q3(MockMR):
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        limit = 20
        parts = list(csv.reader([line.strip()]))[0]
        if parts != ['InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country']:
            yield(parts[7],round(int(parts[3])*float(parts[5]),2))
        #cache management
        if len(self.cache) > limit:
            for i in self.cache:
                yield (i, self.cache[i])
            self.cache.clear()

    def mapper_final(self):
        if len(self.cache) != 0:
            for i in self.cache:
                yield (i, self.cache[i])

    def reducer(self, key, values):
        # key:country, value:(UnitPrice * Quantity)
        yield (key, sum(values))

if __name__ == "__main__":

    q3.run(trace=True)
