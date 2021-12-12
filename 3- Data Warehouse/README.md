#  __PROJECT: DATA WAREHOUSE__

The purpose of this project is to building an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables of SPARKIFYDB.
For that, we design the database, and impelemnted the creation of the cluster and of the database SPARKIFYDB with Boto3 SDK python <font color='#bfbfbf'>*(Mentionned that those ressources must be realize at first, in order to load the data)*</font> And after implemented the ETL pipeline.

## The files
The files in the workspace is necessary for the success of the project.
* **create_tables.py :** This file is used for creating the tables of the database. It containc two functions: <br>
&emsp;&emsp;<font color='#0073e6'>1- drop_tables(cur, conn)</font> with two parameters, for droping tables if they are already exist;
<br>&emsp;&emsp;<font color='#0073e6'>2- create_tables(cur, conn)</font> with two parameters, for creating the tables.
* **dwh.cfg :** This is the config file. It contains all the parameter that will be used: when creating the cluster, the database, and so on.
* **etl.py :** This file is where the etl is implemented. It containts two functions :<br> 
&emsp;&emsp;<font color='#0073e6'>1- load_staging_tables(cur, conn)</font> for taking the query of load table and load data from json to the database.</font> <br>&emsp;&emsp;<font color='#0073e6'>2- insert_tables(cur, conn)</font> for insert data from others json to the other table in the database. 
&emsp;&emsp;<br>Both functions had two parameters for connecting to the database.
* **execute_sparkify_etl.py :** This file represented the principal file of the project.<br>
&emsp;&emsp; <font color='#0073e6'>1- start_operations()</font> For starting the program.
<br>&emsp;&emsp; <font color='#0073e6'>2- create_iam_role()</font> For creating the iam role
<br>&emsp;&emsp; <font color='#0073e6'>3- create_cluster()</font> For creating the cluster and get the IAM role ARN
<br>&emsp;&emsp; <font color='#0073e6'>4- open_incomming_tcp_port()</font> For Opening an incoming TCP port to access the cluster ednpoint
<br>&emsp;&emsp; <font color='#0073e6'>5- getting_endpoint_arn_update_dwh_cfg()</font> For getting endpoint and role arn, and Updating automatically the config file by writing the endpoint and arn informations.
* **REAME.md :** This is the readme file witch contain informations and instructions about the project.
* **sql_queries.py** This file is where all the queries was writen. It's in this file that the **etl.py** file used to take queries.


## The datasets
The datasets that we use to extract the data is 
* **Song Dataset :** Witch contains all the metadata of the songs in JSON format
* **Log Dataset  :** Witch contains so many log files in the JSON format<br>
Informations about the path of those datasets is include in the file **dwh.cfg**, the config file.

## How to run
For running the program, write in the console <font color='#0073e6'>`python execute_sparkify_etl.py`</font>.
A menu will appear, and make a choice of to start the etl or to qui the program.
When you start, just follow in the screen the operations of the creation and configurations of ressources, and follow operations of the ETL. When finishing, If you want to verify in aws, do not make the choice for exiting in the program. Do it after verifying.
