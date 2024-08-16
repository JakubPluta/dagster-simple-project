from dagster_snowflake import SnowflakeIOManager, SnowflakeResource

from dagster import asset

import os
import pandas as pd

import matplotlib.pyplot as plt

# dlt_mongodb_embedded_movies
# dlt_mongodb_movies
@asset(
    deps=[
        "dlt_mongodb_comments",
        "dlt_mongodb_embedded_movies",
    ]
)
def user_engagements(snowflake: SnowflakeResource) -> None:
    query = """
        SELECT 
            movies.title, 
            movies.year as year_released, 
            count(comments._id) as number_of_comments
        FROM comments comments
        join embedded_movies movies 
            on comments.movie_id = movies._id
        group by movies.title, movies.year
        order by number_of_comments desc
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        engagements = cursor.fetch_pandas_all()
    engagements.to_csv("data/user_engagements.csv", index=False)


@asset(deps=["dlt_mongodb_embedded_movies"])
def top_movies_by_month(snowflake: SnowflakeResource) -> None:
    query = """
        select 
            movies.title,
            movies.released,
            movies.imdb__rating,
            movies.imdb__votes,
            genres.value as genres
            
        from embedded_movies movies
        join EMBEDDED_MOVIES__GENRES genres
            on movies._dlt_id = genres._dlt_parent_id
        where released >= '2015-01-01'::date
        and released < '2015-01-01'::date + interval '1 month'
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        top_movies = cursor.fetch_pandas_all()
    top_movies['window'] = '2015-01-01'
    top_movies = top_movies.loc[top_movies.groupby('GENRES')['IMDB__RATING'].idxmax()]
    
    with open("data/top_movies_by_month.csv", "w") as f:
        top_movies.to_csv(f, index=False)

@asset(
    deps=["user_engagement"],
)
def top_movies_by_engagement():
    """
    Generate a bar chart based on top 10 movies by engagement
    """
    movie_engagement = pd.read_csv('data/user_engagements.csv')
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
    plt.savefig('data/top_movie_engagement.png')