Common

- contains functions and connections, which are being used across the whole project.
- dao contains fetch_db_data.py and insert.py, which are used to insert or fetch data from the db.
- dao_client.py contains the session object after creating connection.
- db_config.py contains the connection host, port and db_name.

DataIngestion

- contains the preprocessing scripts for each table.
- services folder contains the helper functions used in Data Ingestion scripts.
- config.py contains the parameters being used in Data Ingestion scripts.
- query.py contains the all the queries used across Data Ingestion scripts.
- test.py - data validation test cases.
- Using run.py, all the preprocessor scrips can be called and data ingestion to the db will start.

DataService

- services folder contains the helper functions used in Data Ingestion scripts.
- src contains api's for BI (app.py) and AI (app_AI.py) systems.
- app.py will start the data services
- run.py contains the base dataframes after joining, to be used to get data for api's
- app_config.py contains port and host in which data services will be hosted.
- fetch_sql_queries.py has all the queries used to create DF's in run.py

Cassandra installation Post Steps

- Change listen_address, rpc_address and seeds to interface api
- in case of connectivity issue, run below mentioned netstat command to check which interface
  and port the server is listening to
  "netstat -tlnp"
- increase the tombstone_failure_threshold:1000000

Steps to run the project in local

- make sure the cassandra is installed and is working in your local
- create tables in cassandra-
    1. open cmd, type cqlsh.
    2. Copy the scripts from common/CricketSimulatorDDL.sql (except truncate statements).
    3. Run the scripts on cqlsh console.
- update the common/db_config.py to point to the local host i.e., HOST=127.0.0.1
- run the main.py file in DataIngestion
- once, the ingestion is completed, trigger app.py to start the DataService