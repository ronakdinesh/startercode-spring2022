from mockmr import MockMR 
import csv

class q1(MockMR):
    
    def mapper(self, key, line):

        parts = list(csv.reader([line.strip()]))[0]
        if parts != ['InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country']:
            yield(parts[7],round(int(parts[3])*float(parts[5]),2))

    def reducer(self, key, values):
        # key:country, value:(UnitPrice * Quantity)
        yield (key, sum(values))

if __name__ == "__main__":

    q1.run(trace=True)