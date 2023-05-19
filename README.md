# ETL for Reddit post

This project is the following of the PoC about [ADHD Reddit posts](https://github.com/cecilegltslmcs/adhd-reddit-analysis).


In this project, a ETL was created by using Python. The aim of the script is :
- To create a client in order to collect data from different chosen subreddits
- To transform the data in order to remove outliers by basing on the numbers of comments.
- To load data in a csv file.

In this implementation, I decided to not removing outliers because the criteria are not suitable in our case. However, the code about this step is still available. 
It is also possible to change the load step in order to load the data in a database.

The main idea of the code came from this [article](https://www.startdataengineering.com/post/code-patterns/).

The library [PRAW](https://praw.readthedocs.io/en/stable/) is used in this script but it can be also possible to used [request](https://fr.python-requests.org/en/latest/).

For the future, this pipeline is going to be automatize to run every week. 

---

## Storage

To store the collected data, a PostgreSQL database will be used.

