### Project for ContikiNG, Node-RED, ApacheSpark technologies

## Description

##### Data Ingestion from the devices living in the COOJA environment (Contiki/IoT):

1. Collect data from IoT devices: device IPs, nationality, age and the information on the groups

##### Reporting to Back-end (NodeRED):
 
1. Recieves message identifing IoT devices -> stores info on people
2. Recieves message identifing groups -> stores info on groups

##### Backend Statistics (NodeRED):
1. Compute statistics for each group

##### Data Analysis (Spark): 
1. Done on a .csv which is written by the backend


## Contiki NG

To detect contacts: **Constant Loss Unit-Disk Graph Model (CL-UDGM)**, which is a wireless propagation model, used to detect when devices are within the same broadcast domain (1-hop distance from each other). The **Routing Protocol** creates atree-shaped topology rooted at the node with direct Internet access. The **Communication Protocol** is UDP.

**Types of Motes**: Three different types of motes involved in the simulation:

1. RPL border-router: This mote serves as the root of the tree-shaped network topology and provides direct Internet access. Also called *man-in-the-middle*. It is configured as  Destination-Oriented Directed Acyclic Graph (DODAG).
2. MQTT-UDP mote: MQTT is a messaging protocol often used in IoT applications. It looks for motes in his neighborhood and once found it communicates with those its IPv6 address. Once the communication arrives, each node checks if the new contact has been with a specific signal node and connect to the MQTT broker to store on the backend the contact.
3. UDP signaler


## NodeRed
Built-in nodes can be used to build flows, which then get deployed on local machine. Flows may be saved and loaded as JSON. Shares features with Dataflow programming, and with the actor model, nodes are reactive actors and data flows as messages. Ex: 1 node generates a timestamp every second, sends it to another node, which prints every timestamp.

**Node Types**:

1. Input nodes (start flow)
2. Output nodes (end flow)
3. Functions (processing nodes)

**Messages** are JavaScript objects, three properties:
1. topic: a description of a channel the message belongs to, it characterizes the message content
2. payload: the actual message content, can be any JavaScript data type, including objects too
3. messageId: a unique identifier within a Node-RED running instance.

Ex message: `{"topic":"Hello",`
`"payload":"Hello World",`
`"_msgid":"7f8400c5.0f32c"}`

**Function nodes** are generic containers for JavaScript code. They receive a message object as input and generate one or more message objects as output. From UI, open function node and write code to be executed: when flow is deployed, when a new message is recieved and when flow stops. One may: override the payload of the incoming message, slice the payload of the incoming message. Multi-output: using JavaScript arrays, size of the array must be the same as the number of outputs configured for the function node.

**Data Sharing**: writing on files locally is one way to share data between different flows/executions. A special `context` object exists for function nodes to retain state across invocations, it applies to individual nodes and is re-initialized when deploying the flow (one needs to use some external file to be retainn state across deployment actions). There also exists way to share data across different flows.

#### Networking in NodeRed
Many functionalities: UDP, HTTP, MQTT. Data structures are JavaScript types, but in any case it will be serialized, for communication.

**UDP**: lightweight protocol, small messages. When receiving UDP messages, we specify what port we listen on. When sending a UDP message, we specify the destination IP address and port. Node-RED provides nodes for UDP messages,  where content is taken from `msg.payload`:
1. receiving `udp in`, need to specify `port`
2. sending `udp out`, need to specify address `127.0.0.1` and `port`

*But what if message for example contains 2 fields?* We use JSON for serialization: there is a built-in node to convert to/from JavaScript objects and JSON. It is called `json`. Can be configured manually. Can also be used to convert back to `JS Object`.

**MQTT**: *subscribers* (consumers) express an interest in data through *topics* or *content*, *publishers* (producers) generate data that is disseminated in a message bus. A message broker matches published data with existing subscriptions.

Properties: no destination IP and port information needed, payload agnostic. Communication becomes data-centric, rather than address-centric, but you need the **broker**! Topics are arranged hierarchically, ex: `iot`, `iot/building21`.

NodeRed nodes are provided to use MQTT as an input by expressing subscriptions, or as an output to publish messages. Messages are *strings*, no need of explict JSON node, everything is done automatically.

**Port where UDP or MQTT is listening is port where COOJA is, this will be the case of the project. Look slides for example**.

-
-

## Implementation - NodeRed
NodeRed is the framework which contains the backend of the project. Three diffent flows processe MQTT messagges incoming from COOJA. MQTT communication follow the publish and subscribe messaging pattern: each mote subscribes to one of three topics. The backend then writes stores all information into two different .csv files: environment.csv and output.csv. The three topics are:

1. **newEnvironmentPerson**: a message containing information regarding a newly spawned mote: *IP*, *nationality*, *age*. The message is processed and, after checking that the IP hasn't been processed yet, is saved as a new line in the environment.csv. **Computing statistics**: whenever a valid message is recieved on this topic, the backend also initializes the  statistics of the newy formed group.
2. **createGroups**: a message containing information on the creation of a group. It features: a *teamLeaderIP* highlihgting the IP of the team leader, a *listofIPs* highlighting the IPs of the group members and an integer named *cardinality*. The message is processed and, after checking that the group hasn't been processed yet, is saved as a new line in the output.csv.
3. **changeCardinality**: similarly to the previous topic, it recives a message containing three objects: *teamLeaderIP*, *listofIPs*, *cardinality*. The cases might be two: either a new member has joined a group (*cardinality* >= 3) or a group has just been dismantled (*cardinality* < 3). In the former case, after checking that the group exists in the database, the line of the .csv describing that group is updated. In the latter case, after checking that the group exists in the database, a timestamp in the *dateEnded* column of line of the .csv describing that group is added. **Computing statistics**: whenever a valid message is recieved on this topic, the backend also computes and updates the statistics of the group.

## Implementation - Spark

The optional task of the project requested that the back-end should also compute:

1. Compute a 7-day moving average of participants' age per nationality, computed for each day.
2. Calculate the percentage increase (with respect to the day before) of the 7-day moving average for each nationality.
3. Determine the top nationalities with the highest percentage increase of the seven days moving average for each day.

To tackle this task, we decided to use a technology known as Apache Spark, and in particular the Spark SQL engine. This engine was choosen as all three requests could be translated in a SQL query to be executed on the output dataset of the NodeRed flows. As pointed out earlier, the backend (i.e. NodeRed) saves the output of the flows on the `output.csv` file, which is therefore the file on top of which the SQL queries will be executed.

The first step of the Spark engine is to import the `output.csv` file and create a dataset: `dataSpark`, which contains all the information that one needs for the three queries.

**Query 1**:
This query is divided into two steps. First of all, the data is filtered such that only the groups that were created in the past 7 days (with respect to "today") are retained. The second query instead computes the "moving average" of the partecipants in the retained groups, by nationality. This second query also adds a `StartDate` and `EndDate` column to highlight the window in which the average was computed.

    Dataset<Row> filteredGroups = dataSpark
                        .filter(expr("dateJoined >= date_sub('" + date + "', 6)"))
                        .filter(expr("dateJoined <= '" + date + "'"));
      

    Dataset<Row> movingAverage = filteredGroups
                            .groupBy("Nationality")
                            .agg(round(avg("Age"), 2).as("movingAverage"))
                            .withColumn("StartDate", lit(date))
                            .withColumn("EndDate", lit(java.time.LocalDate.parse(date).minusDays(6).toString()))
                            .orderBy("Nationality");

**Query 2**:
This query is subdivided into three steps.

1. In a fashion similar to Query 1, filter the groups that joined in past 7 days, but with respect to "yesterday" (and not "today", as in Query 1)
2. Compute the moving average at the previous step
3. Finally, by recollecting the moving average of Query 1, compute the percentage increase between the two moving averages.

This executes the second query.

	Dataset<Row> filteredGroupsDayBefore = dataSpark
                .filter(expr("dateJoined >= date_sub('" + dayBefore + "', 6)"))
                .filter(expr("dateJoined <= '" + dayBefore + "'"));
        
    Dataset<Row> movingAverageDayBefore = filteredGroupsDayBefore
                .groupBy("Nationality")
                .agg(round(avg("Age"), 2).as("movingAverageDayBefore")).orderBy("Nationality");
        
    Dataset<Row> percentageIncrease = movingAverageDayBefore
                .join(movingAverage, "Nationality")
                .withColumn("PercentageIncrease",
                            round((col("movingAverage").divide(col("movingAverageDayBefore")).minus(1)).multiply(100), 2))
                .withColumn("Date", lit(date))
                .orderBy("Nationality")
                .drop("StartDate", "EndDate");

**Query 3**:
For this query, a `WindowSpec` object is used to order the date by the percentage increase at Query 2, in descending order. Secondly, a SQL query extracts the top 1 nationality with the highest percentage increase, for each day.


    WindowSpec windowSpec = Window.partitionBy("Date").orderBy(desc("PercentageIncrease"));

    Dataset<Row> rankedNationalities = percentageIncrease
                        .withColumn("rank", rank().over(windowSpec))
                        .filter(col("rank").leq(1)) // Keep only top 1 nationalities
                        .drop("rank")
                        .select("Date", "Nationality", "PercentageIncrease")
                        .orderBy(col("Date"), desc("PercentageIncrease"));

Finally, the Spark engine also prints to console all of the results of the queries.


-

#### Instructions when operating NodeRed in Docker

To copy .csv from docker container to local environment `docker cp mynodered:/data/output.csv /Users/Carlo/Desktop/POLITECNICO/NSDS/NSDS_Projects_2024/Project1/data`

To initialize `output.csv` inside `/data`: get into docker container `docker exec -it XXX /bin/bash` the navigate to `/data` and `rm output.csv`. Then run `echo "groupID,listOfPeople,nationality,age,dateJoined,lifetime,currentCardinality,minCardinality,maxCardinality,averageCardinality,nChangeCardinality" > output.csv`.
For the environment dataset: `echo "IP,nationality,age" > environment.csv`

Complete command:
`docker exec -it ca7bfbf5c7e4  /bin/bash -c "cd /data && rm output.csv && echo 'groupID,teamLeader,listOfPeople,nationality,age,dateJoined,dateEnded,lifetime,currentCardinality,minCardinality,maxCardinality,averageCardinality,nChangeCardinality' > output.csv && echo "IP,nationality,age" > environment.csv"`

-



### Running Spark in a distributed environment:

`export SPARK_MASTER_HOST=127.0.0.1`

If first time running, go to where spark is installed, in my case: `/Users/Carlo/Documents/spark-3.3.0-bin-hadoop2` and cd into `conf`.

Then execute: `cp spark-defaults.conf.template spark-defaults.conf`.

Make sure that spark-defaults.conf has the following lines uncommented `spark.eventLog.enabled` and `spark.eventLog.dir`

To start master: `./sbin/start-master.sh`

To start one worker: `./sbin/start-worker.sh spark://127.0.0.1:7007`

To start visualization tool: `./sbin/start-history-server.sh`

Access UI at `http://127.0.0.1:8080/`, one should see 1 worker ready.

Going pack to code repo, run `mvn package` which creates a `.jar` file inside the `target` folder.

<!--Lastly, run: `./bin/spark-submit --class it.polimi.middleware.spark.batch.wordcount.WordCount /Users/Carlo/Desktop/POLITECNICO/NSDS/Lectures/apache_spark/NSDS_spark_tutorial/target/spark_tutorial-1.0.jar spark://127.0.0.1:7007 ~/Users/Carlo/Desktop/POLITECNICO/NSDS/Lectures/apache_spark/NSDS_spark_tutorial/`-->

Lastly, run: `./bin/spark-submit --class it.polimi.middleware.spark/Users/Carlo/Desktop/POLITECNICO/NSDS/NSDS_Projects_2024/Project1/target/SparkAnalysis-1.0.jar spark://127.0.0.1:7007 ~/Users/Carlo/Desktop/POLITECNICO/NSDS/NSDS_Projects_2024/Project1/`

Currently not working: `WARN StandaloneAppClient$ClientEndpoint: Failed to connect to master 127.0.0.1:7007`

Even though everything says that the master is up and running: `jps` and also `nano logs/spark-Carlo-org.apache.spark.deploy.master.Master-1-MacBook-Air-di-Carlo.local.out` has outputs shuch as *I have been elected leader! New state: ALIVE: Confirms that the Spark Master has been elected as the leader and is in the ALIVE state, indicating that it's operational.*