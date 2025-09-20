from dagster_snowflake import SnowflakeResource
from dagster import asset

import os
import pandas as pd
import matplotlib.pyplot as plt


@asset(
    deps=["dlt_mongodb_comments", "dlt_mongodb_embedded_movies"]
)
def user_engagement(snowflake: SnowflakeResource) -> None:
    """
    Movie titles and the number of user engagement (i.e. comments)
    """
    query = """
        select
            movies.title,
            movies.year AS year_released,
            count(*) as number_of_comments
        from comments comments
        join embedded_movies movies on comments.movie_id = movies._id
        group by movies.title, movies.year
        order by number_of_comments desc
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        engagement_df = cursor.fetch_pandas_all()

    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    engagement_df.to_csv('data/movie_engagement.csv', index=False)


@asset(
    deps=["dlt_mongodb_embedded_movies"]
)
def top_movies_by_month(snowflake: SnowflakeResource) -> None:
    """
    Top movie genres based on IMBD ratings, partitioned by month
    """
    query = """
        select
            movies.title,
            movies.released,
            movies.imdb__rating,
            movies.imdb__votes,
            genres.value as genres
        from embedded_movies movies
        join embedded_movies__genres genres
            on movies._dlt_id = genres._dlt_parent_id
        where released >= '2015-01-01'::date
        and released < '2015-01-01'::date + interval '1 month'
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        movies_df = cursor.fetch_pandas_all()

    # Find top films per genre
    movies_df['window'] = '2015-01-01'
    movies_df = movies_df.loc[movies_df.groupby('GENRES')['IMDB__RATING'].idxmax()]

    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    movies_df.to_csv('data/top_movies_by_month.csv', index=False)


@asset(
    deps=["user_engagement"]
)
def top_movies_by_engagement():
    """
    Generate a bar chart based on top 10 movies by engagement
    """
    movie_engagement = pd.read_csv('data/movie_engagement.csv')
    top_10_movies = movie_engagement.sort_values(by='NUMBER_OF_COMMENTS', ascending=False).head(10)

    plt.figure(figsize=(10, 8))
    bars = plt.barh(top_10_movies['TITLE'], top_10_movies['NUMBER_OF_COMMENTS'], color='skyblue')

    # Add year_released as text labels
    for bar, year in zip(bars, top_10_movies['YEAR_RELEASED'].astype(int)):
        plt.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2, f'{year}',
                 va='center', ha='left', color='black')

    plt.xlabel('Engagement (comments)')
    plt.ylabel('Movie Title')
    plt.title('Top Movie Engagement with Year Released')
    plt.gca().invert_yaxis()
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    plt.savefig('data/top_movie_engagement.png')
    plt.close()  # Close the figure to free memory