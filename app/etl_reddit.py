import praw
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def connection_to_reddit(personal_script: str, 
                         client_secret: str, 
                         username: str,
                         password: str,
                         user_agent: str) -> praw.reddit.Reddit:
    """_summary_

    Args:
        personal_script (str): _description_
        client_secret (str): _description_
        username (str): _description_
        password (str): _description_
        user_agent (str): _description_

    Returns:
        praw.reddit.Reddit: _description_
    """
    try:
        client = praw.Reddit(
        client_id = personal_script,
        client_secret = client_secret,
        username = username,
        password = password,
        user_agent = user_agent
    )
    except:
        print("Failed to connect to reddit! ")
        
    return client

def extract(subreddit_name: str, 
            limit_post: int,
            client) -> list:
    """_summary_

    Args:
        subreddit_name (str): _description_
        limit_post (int): _description_
        client (_type_): _description_

    Returns:
        list: _description_

    Yields:
        Iterator[list]: _description_
    """
    print("Name of the subreddit: ", subreddit_name)
    print("Extraction...")
    subreddit = client.subreddit(subreddit_name)
    reddit_post = subreddit.hot(limit=limit_post)
    data = []

    for submission in reddit_post:
        yield {
            'id': submission.id,
            'subreddit_name': subreddit_name,
            'title': submission.title,
            'num_comments': submission.num_comments,
            'score': submission.score,
            'text': submission.selftext,
            'url': submission.url,
        }
    
def transform(data: list) -> pd.DataFrame:
    """_summary_

    Args:
        data (list): _description_

    Returns:
        pd.DataFrame: _description_
    """
    print("Transformation...")
    data = pd.DataFrame(data)
    data['id'] = data['id'].values.astype(str)
    data.set_index('id')
    return data

def connection_to_database(string_connection: str) -> sqlalchemy.engine.base.Engine:
    """_summary_

    Args:
        string_connection (str): _description_

    Returns:
        sqlalchemy.engine.base.Engine: _description_
    """
    try:
        engine = create_engine(string_connection)
    except:
        print("Failed to connect to the database! ")
    return engine

def load(data: pd.DataFrame,
         engine) -> str:
    """_summary_

    Args:
        data (pd.DataFrame): _description_
        engine (_type_): _description_

    Returns:
        str: _description_
    """
    print("Loading on database...")
    data.to_sql("reddit_extraction", engine, if_exists='append')
    return 'Data loaded into database!'

def main(subreddit_name: str,
         client: praw.reddit.Reddit,
         engine: sqlalchemy.engine.base.Engine) -> str:
    """_summary_

    Args:
        subreddit_name (str): _description_
        client (praw.reddit.Reddit): _description_
        engine (sqlalchemy.engine.base.Engine): _description_

    Returns:
        str: _description_
    """
    data = extract(subreddit_name, 2500, client)
    transformed_data = transform(data)
    load(transformed_data, engine)
    return 'ETL FINISHED! '

if __name__ == '__main__':
    load_dotenv()
    PERSONAL_SCRIPT = os.getenv("personal_script")
    CLIENT_SECRET = os.getenv("secret")
    USERNAME = os.getenv("username")
    PASSWORD = os.getenv("password")
    USER_AGENT = os.getenv("user_agent")
    STRING_CONNECTION = os.getenv("string_connection_docker")
    
    subreddit = ["Autism", "AutismInWomen"]
    
    client = connection_to_reddit(PERSONAL_SCRIPT, 
                                    CLIENT_SECRET, 
                                    USERNAME, 
                                    PASSWORD,
                                    USER_AGENT)
    
    engine = connection_to_database(STRING_CONNECTION)
    
    for i in subreddit:
        main(i, client, engine)