CREATE DATABASE reddit_posts;

USE reddit_posts;

CREATE TABLE reddit_extraction(
    id VARCHAR(15),
    subreddit_name VARCHAR(255),
    title VARCHAR(255),
    created DATE,
    num_comments INT,
    score INT,
    text VARCHAR(1000),
    URL VARCHAR(255),
    PRIMARY KEY(id)
);