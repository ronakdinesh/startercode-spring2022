from mockmr import MockMR
import csv

class q4(MockMR):
    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        limit = 20
        parts = list(csv.reader([line.strip()]))[0]
        if parts != ['InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country']:
            yield(parts[7],int(parts[3])*float(parts[5]))
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
        total = sum(values)
        
        # key:country, value:(UnitPrice * Quantity)
        yield (key, (sum(values)/total))
    
    

if __name__ == "__main__":

    q4.run(trace=True)
