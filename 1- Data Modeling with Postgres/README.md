
# Project: Data Modeling with Postgres

## Purpose
The purpose of this project is to create a database and an ETL pipeline.

In order to a DataEngineer to analyse all the collected data, it's imporatnt at first to realize
the schema of the database in a way it can be easily to querying data.
That's the reason why star schema is applied, in which we have the fact table songplays
and the dimensions tables like artists, songs, users and time. This type of schema help us to avoid complex
query with less join.

After the schema is done, an ETL Pipeline is necessary in order help the analyse and also to populate the database
with the data of the datasets. And then it will be able to analyse data.


## Runing file
For running the script, i used the terminal on the luncher. I ran the create_tables.py file : python create_tables.py.
If there's no error, i ran secondly the etl.py file : python etl.py


## The files of the project
In the repository, we have six files and one directory which contain the datasets.
 * ```create_tables.py``` is the file that we must run in order to create the database and to connect to it so that after to create the tables.
 * For ```etl.ipynb``` file, we can say that this is the ETL process. Show how to connect to the database and how data was extracted from the datasets in order to 
   populate the tables of the databases after process data or selected specific data for populate other tables.
 * The ```etl.py``` it's for building the pipeline, as a .py file. This pipeline will be run for processing data come from the datasets.
 * the ```README.md``` file it is for add explanation about the project and give the purpose of it, how to execute and give explanations according to the files used on this project.
 * The ```sql_queries.py``` file is the place that we add all the sql script for creating, inserting and selecting data that wil be use in the creating of the ETL process and the pipeline.
 * The ```test.ipynb``` file is used for testing if the pipeline do exactely what it supposed to do.