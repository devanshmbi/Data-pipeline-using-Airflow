Cloud Data Warehouse

Sparkify Analytics

As the trend of custom recommended playlist increases. We are startup group who wants to fullfil their customers needs to provide enhanced and best recommended music playlist according to their mood and category for our customers via music streaming app.

The process started when we started to analyze our users activity, to know them better what kind of songs they listen and construct a real time ETL Workflow, which will parse, analyze, aggregate and Insert data into Postgres Database which will help us in categorizing and enhancing our recommendation models for our customer.

Data Sources:

	1) Log data: s3://udacity-dend/log_data
	2) Song data: s3://udacity-dend/song_data

Schema for staging tables:

The log_data directory contains events files in JSON format. Using the same schema we have created staging_events staging table as follows: 
Table: staging_events

	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4

The log_data directory contains log files in JSON format. Using the same schema we have created staging_songs staging table as follows: 
Table: staging_songs

	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4

We have distributed our tables in form of STAR schema, for better analysis. The list of tables and their fields are as follows:

Fact Table, also known as songplays table will contain records from log data associated with the songs such as:


	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)


Dimension Tables design have been categorized as follows:

1) Users Table - To get all the users from music streaming app, the respective schema for the table is attached below
    
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	level varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    
2) Songs Table - Collections of songs from music database, the respective schema for the table is attached below
    
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	year int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)

3) Artist Table - Collections of artists from music database, the respective schema for the table is attached below
    
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)

4) Time Table - Timestamps of song records played by the user which partitioned by year, month,.etc, the respective schema for the table is attached below
    
    start_time TIMESTAMP PRIMARY KEY distkey sortkey,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
    

The Project Structure is as follows:

sql_queries.py: contains insert sql statements for populating fact and dimensions
create_tables.sql: contains create table sql statements for staging, facts and dimensions tables
stage_redshift.py: customer operator which copies data from S3 into redshift staging tables
load_dimension.py: customer operator which loads data from redshift staging tables into dimensional tables
load_fact.py: customer operator which loads data from redshift staging tables into fact tables
data_quality.py: customer operator which check quality of data, whether there are records inserted or not
data_pipeline_airflow_dag.py: contains DAG definations which defines operators, run tasks as per schedule


Project Description:

For this project, we have hosted our data warehouse on cloud using AWS Redshift as a service. We have stored our log_data and event_data files on S3.
The ETL pipeline is build as follows:
1) copy data from S3 and insert it into two staging tables.
2) Populate the remaining facts and dimensions table from staging tables using insert sql statements.
3) 


Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.


Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.



Project Steps:

1) Write sql queries for create, insert and drop tables into sql_queries.py file and save it.
2) Open console and run create_tables.py file, it will drop exisiting tables, create new tables for the project.
3) Open etl.ipynb notebook and execute step by step to ingest one file of song_data and log_data into database and tables.
4) Open etl.py file and edit as per etl.ipynb step by step to ingest all the data into database and tables.
5) Open console and run etl.py file to ingest data into database and tables.
6) Run test.ipynb to check the results and data integrity.

ETL Pipeline Flow:

1) First open a new configuration file and write redshift connection configuration into the file
2) Read the configuration file and create redshift cluster using python libraries boto3 which is an AWS SDK for python
3) After creating the cluster, check the connection as well as test the connection to database.
4) Edit sql_queries.py file and write SQL statements to drop, create, copy and insert to load data into respective tables.
5) After editing the sql_queries.py file, run create_tables.py from console and it will create tables as per statements.
6) Check into redshift whether tables have been created or not.
7) Run etl.py file to copy data from s3 and populate all the tables as per insert statements, written into sql_queries.py file
8) Perform queries on Redshift or from juypter notebook to check the counts of ingested data.


Author: Devansh Modi