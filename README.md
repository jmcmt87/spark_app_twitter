# spark_app_twitter
## Monitoring Twitter Trends

App for monitoring trends on Twitter, in this case scenario we use the Ukrainean War (2022) as a case scenario,
the project can be used for different purposes just modifying the `config.json` file in `/tweepy_ingestion`, and also the `functions.py` 
in `/tweepy_ingestion/utilities` in order to change the topics that will be created automatically.

### How to start it:

* 1st step: launching the kafka cluster, for that it's necessary to have docker-compose installed, launch it from
`/kafka_cluster_step` using the command:

```
docker-compose up -d
```
* 2nd step: launching the script `/tweepy_ingestion/main.py` to start ingesting data and sending it to the kafka cluster.

* 3rd step: launching the spark application to start processing the data `spark_app/main.py`.

* 4th step: launching the streamlit application in `/streamlit`, for that it's needed to use the command:

```
streamlit run main.py
```

You can use every Pipfile and Pipfile.lock to recreate the **pipenv environment** in the different folders:

```
pipenv shell

python3 main.py
```
The architecture of the App:

!(https://ibb.co/M6v1h4k)
