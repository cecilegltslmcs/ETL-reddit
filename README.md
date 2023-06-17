# Collection of Reddit Post

*Last update: 17th June 2023*

## Table of content

- [General Info](#general-info)
- [Technologies](#technologies)
- [Credits](#credits)
- [Source](#source)

## General Info

The aim of this project is to collect data from the social network Reddit.The library PRAW is used to communicate with the Reddit's API. After requesting Reddit's API, data are transformed into a dataframe by the data pipeline. After these transformations, the data are stored in a SQL Database (PostgreSQL). A Jupyter Notebook is used this database in order to realise analysis and create a Reddit post Classifier. 

![Alt text](illustration/Reddit-collection.png)

## Technologies

- Python
- [PRAW](https://praw.readthedocs.io/en/stable/)
- PostgreSQL

## Credits

- Cecile Guillot

## Source

- [Article from Start Data Engineering](https://www.startdataengineering.com/post/code-patterns/).
