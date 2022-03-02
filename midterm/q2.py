'''Question 3. The previous question asked you two write two separate programs, but why write two when you
can write only one? Write one mapreduce job that, for each airport, counts the number of incoming and
outgoing 
ights. So, for example, if LAX has 3 outgoing 
ights and 4 incoming 
ights, one of the output
lines should look like:
LAX (3, 4)
(don't worry so much about punctuation in the output le, the content is more important). Make sure your
code is ecient (pretend the list of all airports is too large to t in memory). Think carefully about what
messages the mapper needs to send so that the reducer can compute the right answer.
 Put your code in q2.py
 Make sure to add/commit/push. Submit to gradescope.'''

class q2(MockMR):
    def mapper_init(self):
        self.cache= {}

    def mapper(self, key, value):
        if parts != [Airline,Source,Destination,Stops]:
            #key: Airline, value: ()
            yield key, value
        #cache management
        if len(self.cache) > limit:
            for i in self.cache:
                yield (i, self.cache[i])
            self.cache.clear()


     def mapper_final(self):
        if len(self.cache) != 0:
            for i in self.cache:
                yield (i, self.cache[i])



    def reducer(self, key, values_iterator):
        yield key, sum(values_iterator)
    def reducer_final(self):
        pass
if __name__ == "__main__":

    q2.run(trace=True)