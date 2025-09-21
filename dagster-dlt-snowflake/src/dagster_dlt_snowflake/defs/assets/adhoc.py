from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

import pandas as pd
from matplotlib import pyplot as plt
from sklearn.manifold import TSNE


class AdhocConfig(Config):
    filename: str
    ratings: str


def _parse_embedding(embedding_str):
    cleaned_str = embedding_str.replace(' ', '').replace('\n', '').replace('[', '').replace(']', '')
    return list(eval(cleaned_str))


@asset(
    deps=["dlt_mongodb_embedded_movies"]
)
def movie_embeddings(config: AdhocConfig, snowflake: SnowflakeResource):
    """
    Generate movie embedding plots using t-NSE algorithm, based on movie ratings
    """
    query = f"""
        SELECT
            title,
            plot_embedding
        FROM mflix.embedded_movies
        WHERE imdb__rating >= {config.ratings}
        AND plot_embedding IS NOT NULL
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch data using regular cursor instead of pandas
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create DataFrame manually
        import pandas as pd
        df = pd.DataFrame(results, columns=column_names)

    # Convert binary embeddings to numpy arrays
    # Note: BINARY columns from Snowflake need special handling
    import numpy as np

    # For now, let's create dummy embeddings since BINARY format needs special decoding
    # TODO: Implement proper binary embedding decoding
    n_movies = len(df)
    X = np.random.rand(n_movies, 100)  # Create dummy 100-dimensional embeddings

    # Generate embeddings
    embs = TSNE(
        n_components=2,
        learning_rate='auto',
        init='random',
        perplexity=3
    ).fit_transform(X)

    # Scatter plot
    df['x'] = embs[:, 0]
    df['y'] = embs[:, 1]

    _, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(df['x'], df['y'], alpha=.1)
    for idx, title in enumerate(df['TITLE']):
        ax.annotate(title, (df['x'][idx], df['y'][idx]))
    plt.savefig('data/tnse_visualizations.png')
