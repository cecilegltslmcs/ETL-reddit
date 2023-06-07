import praw
import authentification_token as auth
import pandas as pd
import datetime

def extract():
    client = praw.Reddit(
        client_id = auth.personal_script,
        client_secret = auth.secret,
        username = auth.username,
        password = auth.password,
        user_agent = "my user agent"
    )

    subreddit = client.subreddit("Autism")
    reddit_post = subreddit.hot(limit=2500)
    data = []
    for submission in reddit_post:
        data.append(
            {
                'title': submission.title,
                'score' : submission.score,
                'id': submission.id,
                'url': submission.url,
                'num_comments' : submission.num_comments,
                'created' : submission.created,
                'text' : submission.selftext                
            }
        )
    return data

def transform(data):
    data = pd.DataFrame(data)
    data['created'] = pd.to_datetime(data['created'])
    return data

def load(data, dataset_name):
    data.to_csv("data/" + dataset_name + ".csv")

def main():
    print("Extraction...")
    data = extract()
    print("Transformation...")
    transformed_data = transform(data)
    print("Loading...")
    load(transformed_data, "autism-test-transform")
    print("End!")

if __name__ == '__main__':
    main()