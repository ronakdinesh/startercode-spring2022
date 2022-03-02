from mockmr import MockMR
import csv

class q1(MockMR):

    def mapper(self, key, line):

        parts = list(csv.reader([line.strip()]))[0]
        if parts != ['InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country']:
            country = parts[7]
            quantity = int(parts[3])
            unitprice = float(parts[5])
            total_money = (quantity * unitprice)
            yield country,(quantity,total_money)

    def reducer(self, country, data):
        quantity = 0
        cost = 0
        for i in data:
            quantity += i[0]
            cost += i[1]
            yield country, (quantity,cost)



if __name__ == "__main__":

    q1.run(trace=True)