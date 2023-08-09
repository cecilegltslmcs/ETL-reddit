import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_env


def connect_to_database(string_connection):
    try:
        engine = create_engine(string_connection)
    except:
        print("Failed to connect to the database! ")
    return engine

def open_data_csv(path):
    df = pd.read_csv(path)
    return df

def transform_data_from_csv(data):
    data.drop(["created","Unnamed: 0"], axis=1, inplace=True)
    data['id'] = df['id'].values.astype(str)
    data['subreddit_name'] = 'Autism'
    data['date'] = pd.to_datetime(data['date'], unit='s')
    data.drop_duplicates('id', inplace=True)
    data.drop([0, 1], inplace=True)

def load_in_database(transformed_data, engine):
    print("Loading on database...")
    try:
        data.to_sql(transformed_data, engine, if_exists='append')
        return 'Data loaded into database!'
    except:
        return 'Error when loading data in database! '

def main(path):
    try:
        data = open_data_csv(path)
        transformed_data = transform_data_from_csv(data)
        load_in_database(transformed_data, engine)
        return 'ETL FINISHED!'
    except:
        return 'Error during ETL realisation'

if __name__ == '__main__':
    load_env()
    STRING_CONNECTION = os.getenv("string_connection")
    engine = connect_to_database(STRING_CONNECTION)
    main(path)