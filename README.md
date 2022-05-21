# spark_app_twitter
## Monitoring Twitter Trends

App for monitoring trends on Twitter, we use the Ukranian War (2022) as a case scenario,
the project can be used for different purposes just modifying the `config.json` file in `/tweepy_ingestion`, and also the `functions.py` 
in `/tweepy_ingestion/utilities` in order to change the topics that will be created automatically.

### Architecture schema:

![title](./Untitled-Diagram-2-2-8-drawio.png)

### Code map
```
spark_twitter_app
|
|__kafka_cluster_step
|		|__docker-compose.yaml
|	
|__tweepy_ingestion
|		|__tweepy_kafka.py
|		|__config.json
|		|__Pipfile
|		|__Pipfile.lock
|		|__utilities
|			  |__ __init__.py
|			  |__functions.py
|
|__spark_app
|         |__spark_app.py
|	  |__config.json
|	  |__Pipfile
|         |__Pipfile.lock
|         |__functions
|		   |__ __init__.py
|		   |__functions.py
|
|__streamlit
     |__streamlit_app.py
     |__config.json
     |__Pipfile
     |__Pipfile.lock
     |__utilities
     |	      |__ __init__.py
     |        |__utilities.py
     |
     |__charts
	    |__ __init__.py
	    |__charts.py

```

### Start:

* 1st step: launching the kafka cluster, for that it's necessary to have docker-compose installed, launch it from
`/kafka_cluster_step` using the command:

```
docker-compose up -d
```
* 2nd step: launching the script `/tweepy_ingestion/main.py` to start ingesting data and sending it to the kafka cluster.

* 3rd step: launching the spark application to start processing the data `/spark_app/main.py`.

* 4th step: launching the streamlit application in `/streamlit`, for that it's needed to use the command:

```
streamlit run main.py
```

You can use every Pipfile and Pipfile.lock to recreate the **pipenv environment** in the different directories:

```
pipenv sync

pipenv shell

python3 main.py
```
### Requirementes

* Twitter Developer Account
* Twitter API v2 Bearer Token
* AWS S3 Credentials
* AWS bucket
* MongoDB Atlas cluster

# Screenshots

![title](./screenshot_app.png)
