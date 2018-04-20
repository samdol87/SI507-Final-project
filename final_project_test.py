import unittest
import sqlite3
from final_project import *


class TestDatabase(unittest.TestCase):

    def test_movies_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = 'SELECT MovieName FROM Movies'
        results = cur.execute(sql)
        result_list = results.fetchall()

        self.assertIn(('The Killer',), result_list)
        self.assertEqual('Touch of Evil', result_list[632][0])
        self.assertEqual(len(result_list), 1583)

        sql = '''
            SELECT Ranking, Genre
            FROM Movies
            WHERE MovieName="Her"
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()


        self.assertEqual(result_list[0][0], 23)
        self.assertEqual(result_list[1][1], "Science Fiction & Fantasy")
        self.assertEqual(result_list[2][1], "Special Interest")
        self.assertEqual(result_list[2][0], 7)

        conn.close()


    def test_ratings_table(self):
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        sql = '''
            SELECT AudienceRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            WHERE MovieName = "Wonder Woman"
            '''
        results = cur.execute(sql)
        result_list = results.fetchall()

        self.assertEqual(result_list[0][0], 4.3)
        self.assertEqual(len(result_list), 3)


        sql = '''
            SELECT TomatoRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            WHERE MovieName="Her"
        '''
        results = cur.execute(sql)
        result_list = results.fetchall()


        self.assertEqual(result_list[0][0], 8.5)
        self.assertEqual(len(result_list), 3)

        conn.close()

class TestRatingSearch(unittest.TestCase):

    def test_single_rating(self):

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        result = get_single_movie_rating("Alien")
        result2 = get_single_movie_rating("Alien", False)

        self.assertEqual(result, 3.9)
        self.assertEqual(result2, 9)
        self.assertEqual(len(str(result)), 3)

    def test_genre_rating(self):

        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        result = get_genre_avg_rating("Drama")
        result2 = get_genre_avg_rating("Drama", False)

        self.assertEqual(round(result,2), 4.12)
        self.assertEqual(round(result2,2), 8.79)
        self.assertEqual(len(str(result)), 17)


unittest.main()
