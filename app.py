from flask import Flask
import utils

app = Flask(__name__)


@app.route('/movie/<title>')
def search_movie(title):
    movie = utils.get_movie_by_title(title)
    movie_id = movie[0]

    return utils.get_data_by_id(movie_id)


@app.route('/movie/<start_year>/to/<finish_year>')
def movie_between_years(start_year, finish_year):
    return utils.get_movies_between_years(start_year, finish_year)


@app.route('/rating/<category>')
def movie_by_rating(category):
    return utils.get_movie_by_rating(category)


@app.route('/genre/<genre>')
def movie_by_genre(genre):
    titled_genre = genre.title()
    return utils.get_movies_by_genre(titled_genre)


if __name__ == '__main__':
    app.run()
