import requests
import json
from bs4 import BeautifulSoup
import sqlite3 as sqlite
import csv
import plotly.plotly as py
import plotly.graph_objs as go

# 1. Caching

CACHE_FNAME = 'rt_cache.json'
baseurl = 'https://www.rottentomatoes.com'
top_movie_url = '/top'
url_movie = baseurl + top_movie_url

try:
    cache_file = open(CACHE_FNAME, 'r')
    cache_contents = cache_file.read()
    CACHE_DICTION = json.loads(cache_contents)
    cache_file.close()

except:
    CACHE_DICTION = {}


def get_unique_key(url):
  return url


def make_request_using_cache(url):
    unique_ident = get_unique_key(url)

    if unique_ident in CACHE_DICTION:
        print("Getting cached data...")
        return CACHE_DICTION[unique_ident]

    else:
        print("Making a request for new data...")
        # Make the request and cache the new data
        resp = requests.get(url)
        CACHE_DICTION[unique_ident] = resp.text
        dumped_json_cache = json.dumps(CACHE_DICTION)
        fw = open(CACHE_FNAME,"w")
        fw.write(dumped_json_cache)
        fw.close() # Close the open file
        return CACHE_DICTION[unique_ident]



# 2. Web Scraping from Rotten Tomatoes

class Movie():

    def __init__(self, title, rating, genre, ranking):
        self.title = title
        self.rating = rating
        self.genre = genre
        self.ranking = ranking

def get_genre_url_list():

    page_text = make_request_using_cache(url_movie)
    page_soup = BeautifulSoup(page_text, 'html.parser')

    page_genrelist = page_soup.find('ul', class_ = "genrelist")
    genrelist_a_tag = page_genrelist.find_all('a')

    movie_genre_url_list = []

    for item in genrelist_a_tag:
        genre_url = baseurl + item['href']
        movie_genre_url_list.append(genre_url)

    return movie_genre_url_list

def grab_17_movie_genres():

    page_text = make_request_using_cache(url_movie)
    page_soup = BeautifulSoup(page_text, 'html.parser')

    page_genrelist = page_soup.find('ul', class_ = "genrelist")
    genrelist_a_tag = page_genrelist.find_all('a')

    movie_genre_list = []

    for item in genrelist_a_tag:
        movie_genre = item.string.strip()[8:]
        movie_genre = movie_genre.replace("Movies", "")
        movie_genre = movie_genre.rstrip()
        movie_genre_list.append(movie_genre)

    return movie_genre_list

def grab_100_movies_by_genre():

    genre_list = get_genre_url_list()

    final_movie_list = []
    for item in genre_list:
        top_movie_page_text = make_request_using_cache(item)
        top_movie_page_soup = BeautifulSoup(top_movie_page_text, 'html.parser')

        table_tag = top_movie_page_soup.find('table', class_='table')
        table_a_tag = table_tag.find_all('a')

        movie_list = []

        for tags in table_a_tag:
            movie_name = tags.string.strip()
            movie_name = movie_name[0:-7]
            movie_list.append(movie_name)

        final_movie_list.append(movie_list)

    return final_movie_list

def grab_movies_ranking():

    genre_list = get_genre_url_list()

    final_ranking_list = []
    for item in genre_list:
        top_movie_page_text = make_request_using_cache(item)
        top_movie_page_soup = BeautifulSoup(top_movie_page_text, 'html.parser')

        table_tag = top_movie_page_soup.find('table', class_='table')
        table_td_tag = table_tag.find_all('td', class_ = 'bold')

        ranking_list = []

        for tags in table_td_tag:
            temp_str = tags.string.strip()
            temp_str = temp_str.replace(".", "")
            ranking_list.append(temp_str)

        final_ranking_list.append(ranking_list)

    return final_ranking_list

def grab_movie_rating():

    genre_list = get_genre_url_list()
    movie_url_list = []

    for item in genre_list:
        top_movie_page_text = make_request_using_cache(item)
        top_movie_page_soup = BeautifulSoup(top_movie_page_text, 'html.parser')

        table_tag = top_movie_page_soup.find('table', class_='table')
        table_a_tag = table_tag.find_all('a')

        for tags in table_a_tag:
            temp_url = baseurl + tags['href']
            movie_url_list.append(temp_url)

    other_info_list = []

    tomato_rating_list = []
    audience_rating_list = []

    for url in movie_url_list:
        each_movie_page_text = make_request_using_cache(url)
        each_movie_page_soup = BeautifulSoup(each_movie_page_text, 'html.parser')

        div_tag_2 = each_movie_page_soup.find('div', class_ = 'audience-info hidden-xs superPageFontColor')
        div_tag_3 = each_movie_page_soup.find('div', class_ = 'superPageFontColor')

        audience_rating_temp = div_tag_2.text.strip()
        audience_rating_temp = audience_rating_temp.split('/')

        audience_rating_list.append(audience_rating_temp[0][-3:])

        tomato_rating_temp = div_tag_3.text.strip()
        tomato_rating_temp = tomato_rating_temp.split('/')

        tomato_rating_list.append(tomato_rating_temp[0][-3:])


    other_info_list.append(audience_rating_list)
    other_info_list.append(tomato_rating_list)


    return other_info_list


# movie_name_list_by_genre = grab_100_movies_by_genre()
# movie_genre_list = grab_17_movie_genres()
# movie_rating_list = grab_movie_rating()


# 3. Create Database and insert into tables

DBNAME = 'rottentomatoes.db'

def create_rt_db():

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS 'Movies'
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'Movies' (
        'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
        'Ranking' INTEGER NOT NULL,
        'MovieName' TEXT NOT NULL,
        'Genre' TEXT NOT NULL,
        'GenreId' INTEGER NOT NULL
        );
    '''

    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Rating'
    '''
    cur.execute(statement)


    statement = '''
        CREATE TABLE 'Rating' (
        'MovieId' INTEGER NOT NULL,
        'GenreId' INTEGER NOT NULL,
        'AudienceRating' DECIMAL NOT NULL,
        'TomatoRating' DECIMAL NOT NULL
        );
    '''

    cur.execute(statement)

    movie_name_list = grab_100_movies_by_genre()
    movie_genre_list = grab_17_movie_genres()
    movie_ranking_list = grab_movies_ranking()
    movie_rating_list = grab_movie_rating()
    movie_genre_id_list = []

    for i in range(0, len(movie_genre_list)):
        movie_genre_id_list.append(i+1)

    for i in range(0, len(movie_genre_list)):
        for y in range(0, len(movie_name_list[i])):
            insertion = (None, movie_ranking_list[i][y], movie_name_list[i][y], movie_genre_list[i], movie_genre_id_list[i])
            statement = '''
                INSERT INTO Movies VALUES (?, ?, ?, ?, ?)
            '''
            cur.execute(statement, insertion)

    movie_id_list = []
    movie_genre_id_list2 = []

    for i in range(0, len(movie_genre_list)):
        for y in range(0, len(movie_name_list[i])):
            movie_genre_id_list2.append(i+1)

    for item in movie_name_list:
        for i in range(0, len(item)):

            query = '''
                SELECT Id
                FROM Movies
                WHERE MovieName =?
            '''
            params = (item[i], )

            cur.execute(query, params)
            movie_id_list.append(cur.fetchone()[0])

    for i in range(0, len(movie_rating_list[0])):
        insertion = (movie_id_list[i], movie_genre_id_list2[i], movie_rating_list[0][i], movie_rating_list[1][i])
        statement = '''
            INSERT INTO Rating VALUES(?, ?, ?, ?)
        '''
        cur.execute(statement, insertion)

    conn.commit()
    conn.close()
    print("Created Rotten Tomatoes DB")

# create_rt_db()

# 4.Get Rating values from DB

def get_single_movie_rating(movie_name, audience = True):

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    params = (movie_name,)

    if audience == True:
        query = '''
            SELECT DISTINCT AudienceRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            WHERE MovieName = ?
        '''

    else:
        query = '''
            SELECT DISTINCT TomatoRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            WHERE MovieName = ?
        '''
    try:
        cur.execute(query, params)
        result = cur.fetchone()[0]
        return result
    except:
        print("Cannot find the movie name from the DB")

def get_genre_avg_rating(genre_name, audience = True):

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    params = (genre_name, )

    if audience == True:
        query = '''
            SELECT AVG(AudienceRating)
            FROM Rating
            JOIN Movies
            ON Movies.GenreId = Rating.GenreId
            WHERE Movies.Genre = ?
        '''

    else:
        query = '''
            SELECT AVG(TomatoRating)
            FROM Rating
            JOIN Movies
            ON Movies.GenreId = Rating.GenreId
            WHERE Movies.Genre = ?
        '''
    try:
        cur.execute(query, params)
        result = cur.fetchone()[0]
        return result
    except:
        print("Cannot find the genre from the DB")


# get_single_movie_rating("Wonder Woman", audience=False)
# get_genre_avg_rating("Drama")

# 5. Draw Bar Charts using Plotly

def plot_avg_rating_by_genre(audience = True):

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    x = []
    y = []

    if audience == True:

        query = '''
            SELECT Movies.Genre, AVG(AudienceRating)
            FROM Rating
            JOIN Movies
            ON Rating.GenreId = Movies.GenreId
            GROUP BY Movies.Genre
            ORDER BY AudienceRating DESC
        '''

        cur.execute(query)

        for row in cur:
            x.append(row[0])
            y.append(row[1])
    else:

        query = '''
            SELECT Movies.Genre, AVG(TomatoRating)
            FROM Rating
            JOIN Movies
            ON Rating.GenreId = Movies.GenreId
            GROUP BY Movies.Genre
            ORDER BY TomatoRating DESC
        '''

        cur.execute(query)

        for row in cur:
            x.append(row[0])
            y.append(row[1])

    data = [go.Bar(
                x=x,
                y=y,
                text=x,
                textposition = 'auto',
                marker=dict(
                    color='rgb(158,202,225)',
                    line=dict(
                        color='rgb(8,48,107)',
                        width=1.5),
                ),
                opacity=0.6
            )]

    py.plot(data, filename='bar-direct-labels')

def plot_rating_top_movies(limit = 10, audience = True):

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    x = []
    y = []

    if audience == True:

        params = (limit, )

        query = '''
            SELECT DISTINCT MovieName, Rating.AudienceRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            ORDER BY AudienceRating DESC
            LIMIT ?
        '''

    else:

        params = (limit, )

        query = '''
            SELECT DISTINCT MovieName, Rating.TomatoRating
            FROM Rating
            JOIN Movies
            ON Movies.Id = Rating.MovieId
            ORDER BY TomatoRating DESC
            LIMIT ?
        '''
    cur.execute(query, params)

    for row in cur:
        x.append(row[0])
        y.append(row[1])

    data = [go.Bar(
                x=x,
                y=y,
                text=x,
                textposition = 'auto',
                marker=dict(
                    color='rgb(158,202,225)',
                    line=dict(
                        color='rgb(8,48,107)',
                        width=1.5),
                ),
                opacity=0.6
            )]

    py.plot(data, filename='bar-direct-labels')

# plot_avg_rating_by_genre(True)
# plot_rating_top_movies(15, False)

# 6. Interactivity

if __name__ == '__main__':
    menu = "Welcome to Rotten Tomatoes Database."

    print(menu)
    inp = input("\nPlease select the number:\n1. Search for Movie Rating information\n2. Display Rating Information on Bar Chart\n3. Exit\n")
    while (inp != '3'):
        if inp == '1':
            inp2 = input("How would you like to search rating information? Select either Movie or Genre (Please enter exit if you want to end the application): \n")
            if inp2 == "Movie":
                inp3 = input("Which Rating would you like to choose? Select either Audience or Tomato: \n")
                inp4 = input("Select the name of the movie: ")
                if inp3 == "Audience":
                    result = get_single_movie_rating(str(inp4), audience = True)
                    print("\n", str(inp4), "Audience Rating: ", result, "\n")
                elif inp3 == "Tomato":
                    result = get_single_movie_rating(str(inp4), audience = False)
                    print("\n", str(inp4), "Tomato Rating: ", result, "\n")
                else:
                    print("Wrong input")
            elif inp2 == "Genre":
                inp3 = input("Which Rating would you like to choose? Select either Audience or Tomato: \n")
                inp4 = input("Select the name of the genre: \nAction & Adventure\nAnimation\nArt House & International\nClassics\nComedy\nDocumentary\nDrama\nHorror\nKids & Family\nMusic & Performing Arts\nMystery & Suspense\nRomance\nScience Fiction & Fantasy\nSpecial Interest\nSports & Fitness\nTelevision\nWestern\n")
                if inp3 == "Audience":
                    result = get_genre_avg_rating(str(inp4), audience = True)
                    print("\n", str(inp4), "Average Audience Rating: ", result, "\n")
                elif inp3 == "Tomato":
                    result = get_genre_avg_rating(str(inp4), audience = False)
                    print("\n", str(inp4), "Average Tomato Rating: ", result, "\n")
                else:
                    print("Wrong input")
            elif inp2 == "exit":
                break
        elif inp == '2':
            inp2 = input("How would you like to display the rating information? Select either Movie or Genre (Please enter exit if you want to end the application): \n")
            if inp2 == "Movie":
                inp3 = input("Which Rating would you like to choose? Select either Audiece or Tomato: \n")
                inp4 = input("How many results would you like to display? Choose an integer: \n")
                if inp3 == "Audience":
                    plot_rating_top_movies(int(inp4), audience = True)
                elif inp3 == "Tomato":
                    plot_rating_top_movies(int(inp4), audience = False)
                else:
                    print("Wrong Input")
            elif inp2 == "Genre":
                inp3 = input("Which Rating would you like to choose? Select either Audiece or Tomato: \n")
                if inp3 == "Audience":
                    plot_avg_rating_by_genre(audience = True)
                elif inp3 == "Tomato":
                    plot_avg_rating_by_genre(audience = False)
                else:
                    print("Wrong Input")
            elif inp2 == "exit":
                break
