import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import authentification_token as auth

STRING_CONNECTION = auth.string_connection

engine = create_engine(STRING_CONNECTION)
df = pd.read_csv("data/autism_all.csv")
df.drop(["created","Unnamed: 0"], axis=1, inplace=True)
df['id'] = df['id'].values.astype(str)
df['subreddit_name'] = 'Autism'
df.drop_duplicates('id', inplace=True)
df.drop([0, 1], inplace=True)
df.to_sql("reddit_extraction", engine, if_exists='replace')