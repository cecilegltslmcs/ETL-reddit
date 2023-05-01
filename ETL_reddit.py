import praw
import authentification_token as auth
import pandas as pd


def extract():
    client = praw.Reddit(
        client_id = auth.personal_script,
        client_secret = auth.secret,
        username = auth.username,
        password = auth.password,
        user_agent = "my user agent"
    )

    subreddit = client.subreddit("autism")
    reddit_post = subreddit.hot(limit=25000)
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
    num_comments = [post.get('num_comments') for post in data]

    mean_num_comment = sum(num_comments) / len(num_comments)
    std_num_comments = (
        sum([(x -  mean_num_comment) ** 2 for x in num_comments])
        / len(num_comments)
    ) ** 0.5
    return [post
            for post in data
            if post.get("num_comments") > mean_num_comment + 2 * std_num_comments
            ]

def load(data, dataset_name):
    data = pd.DataFrame(data)
    data.to_csv("data/"+dataset_name+".csv")

def main():
    data = extract()
    #transformed_data = transform(data)
    load(data, "autism")

if __name__ == '__main__':
    main()