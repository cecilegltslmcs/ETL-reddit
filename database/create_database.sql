CREATE DATABASE reddit_posts;

USE reddit_posts;

CREATE TABLE reddit_extraction(
    id VARCHAR(20),
    subreddit_name VARCHAR(255),
    title VARCHAR(255),
    num_comments INT,
    date DATE,
    score INT,
    text VARCHAR(1000),
    URL VARCHAR(255),
    PRIMARY KEY(id)
);