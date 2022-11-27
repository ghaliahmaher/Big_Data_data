
from mrjob.job import MRJob
from mrjob.job import MRStep

class Count_Movies(MRJob):
    # Define single mapreduce phase
    def steps(self):
        return [MRStep(mapper=self.mapper_get_ratings,reducer=self.reducer_count_ratings)]

    def mapper_get_ratings(self, _, line):
        (userID,program_name,duration_in_hour, program_class,genre, hd,original_name,commercial,family,score) = line.split('\t')
        yield original_name, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Count_Movies.run()
