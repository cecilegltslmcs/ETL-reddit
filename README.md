# ETL for Reddit post

The main idea of the code came from this [article](https://www.startdataengineering.com/post/code-patterns/).

For my use, I change some part :
- the transform part in order to transform timestamp into datetime
- the load part in order to use PostgreSQL instead of sqlite.

The library [PRAW](https://praw.readthedocs.io/en/stable/) is used in this script but it can be also possible to used [request](https://fr.python-requests.org/en/latest/).

## Storage

A PostgreSQL is used to store the collected data. For the moment, this database is composed by one table. 
