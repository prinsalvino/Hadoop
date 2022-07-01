from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsSumMovie (MRJob):
        def steps(self):
                return [
                        MRStep(mapper=self.mapper_get_movies_w_rating,
                                reducer=self.reducer_sum_ratings)
                ]

        def mapper_get_movies_w_rating(self, _, line):
                (userID, movieID, rating, timestamp) = line.split('\t')
                yield movieID, int(rating)

        def reducer_sum_ratings (self, movieID, rating):
                yield movieID, sum(rating)

if __name__ == '__main__':
    RatingsSumMovie.run()
