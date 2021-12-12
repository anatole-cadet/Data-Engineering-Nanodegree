# <b>Project: Data Lake</b> <hr>


### The purpose of this database
At first, we must say that this project concern the music streaming startup named SPARKIFY.
Their is a data warehouse for that and the number of users growing up. So, as they would like to 
move the data warehouse to a data lake; we can say that the purpose of that is to allow analysis and
find what song users listening.

### State and justify the database schema design and ETL pipeline
When we analyze the dataset, we noticed that there is only one format of data. But, we mentionned also that those data
(song data, log data) are partioned in the s3 storage.
The database schema that we have is very appropriate for analysis. When we finished to understand the needs and understand
the log data and the songs data, we can talk about the fact and the dimention table which are important for designing a schema.
So,the songsplays table that we have, is the fact table that can contain all the foreign key of the dimentionals tables, contains also some numerical values like length.
The dimenssions tables that we have represents or explain exactely the context of the fact. FOr exemple the user's table explain the "Who", the time's table the "When" and the songs's table the "What"; this the reason being of dimenssionals's tables; and our schema design reflect well this aproch.

For the ETL pipeline, two functions are importants and was created for processing data which come from the dataset.
The functions *process_song_data* and the *process_log_data*; with then we connect to the dataset in order to extract the data of all .json files to create a spark dataframe. And after we used spark's functions to filter or select data according to the dimenssion table that we have, use functions also for extracting columns, prepare data, and create data depending a dimentional table mentioned in the schema design. After processing data; we back the data to s3 by dimmentional and fact table name.