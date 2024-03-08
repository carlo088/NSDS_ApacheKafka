### NSDS Project for ContigiNK, Node-RED, ApacheSpark technologies

## Description

##### Data Ingestion from the devices living in the COOJA environment (Contiki/IoT):

1. Collect data from IoT devices: device IDs, nationality, age and the date they joined the environment

##### Reporting to Back-end (NodeRED):
 
1. Process data to identify contacts and form groups -> report to backed
2. Detect when a group is formed or when its cardinality changes (e.g., when someone joins or leaves) -> report to backed

##### Backend Statistics (NodeRED):
1. Compute statistics for each group


##### Data Analysis (Spark): 
1. Done on a .csv which is updated every day by the backend


### Contiki NG

To detect contacts: **Constant Loss Unit-Disk Graph Model (CL-UDGM)**, which is a wireless propagation model, used to detect when devices are within the same broadcast domain (1-hop distance from each other).

The **Routing Protocol** creates atree-shaped topology rooted at the node with direct Internet access.

The **Communication Protocol** is UDP. ???

**Types of Motes**: Three different types of motes involved in the simulation:

1. RPL border-router: This mote serves as the root of the tree-shaped network topology and provides direct Internet access. Also called *man-in-the-middle*. It is configured as  Destination-Oriented Directed Acyclic Graph (DODAG).
2. MQTT-UDP mote: MQTT is a messaging protocol often used in IoT applications. It looks for motes in his neighborhood and once found it communicates with those its IPv6 address. Once the communication arrives, each node checks if the new contact has been with a specific signal node and connect to the MQTT broker to store on the backend the contact.
3. UDP signaler: ??


### NodeRed
Built-in nodes can be used to build flows, which then get deployed on local machine. Flows may be saved and loaded as JSON. Shares features with Dataflow programming, and with the actor model, nodes are reactive actors and data flows as messages.
Ex: 1 node generates a timestamp every second, sends it to another node, which prints every timestamp.


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

**Function nodes** are generic containers for JavaScript code. They receive a message object as input and generate one or more message objects as output.

From UI, open function node and write code to be executed: when flow is deployed, when a new message is recieved and when flow stops.

Ex of function with 2 outputs (meaning an array of 2 elements): 
```
if (msg.payload % 2==0) {    msg.payload = ”Time is even”;
    return [msg, null];} else {    msg.payload = ”Time is odd”;
    return [null, msg];}return [null, null];
```

One may: override the payload of the incoming message, slice the payload of the incoming message.

Multi-output: using JavaScript arrays, size of the array must be the same as the number of outputs configured for the function node.

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

## Implementation - NodeRed
Backend of project. Flow recieves and processes messagges, MQTT communication follow the publish and subscribe messaging pattern: each mote subscribe to a topic.

Backend recieves:

1. Message on **formation of a group**
	- topic: groups
	- payload: list of 3 people
2. Message on **change of cardinality**
	- topic: cardinality
	- payload: list of n people?

Open Questions (suppose Contiki, like in the repo, sends contacts between devices i.e. list of n people):

1. NodeRed recieves only 1 type of message pr 2?
2. Case of 2 messages, what does 1st message contain? Each person is an object containing: deviceID, nationality, age? And also cardnality is part of the message?
3. For topic cardinality, shall I recieve another list of people (One would rather not introduce a groupID)?
4. It is enough to use only 1 database?
5. In case groupID: who creates and holds group ID? How to distinguish groups?

-

Two event-driven flows, one for `group` and other for `cardinality`. For both first node is MQTT reciever.

To copy .csv from docker container to local environment `docker cp mynodered:/data/output.csv /Users/Carlo/Desktop/POLITECNICO/NSDS/NSDS_Projects_2024/Project1/data`

To initialize `output.csv` inside `/data`: get into docker container `docker exec -it XXX /bin/bash` the navigate to `/data` and `rm output.csv`. Then run `echo "groupID,listOfPeople,nationality,age,dateJoined,lifetime,currentCardinality,minCardinality,maxCardinality,averageCardinality,nChangeCardinality" > output.csv`.

`docker exec -it ca7bfbf5c7e4  /bin/bash -c "cd /data && rm output.csv && echo 'groupID,listOfPeople,nationality,age,dateJoined,lifetime,currentCardinality,minCardinality,maxCardinality,averageCardinality,nChangeCardinality' > output.csv"`


Message: `{"topic":"Hello",`
`"payload":"Hello World",`
`"_msgid":"7f8400c5.0f32c"}`


## How to Run

COOJA is the Contiki-NG simulator. Basic node type is the **mote**, it runs as a C process within the COOJA JVM's istance.

To run: `cd` into where contiki lives, then `cd tools/cooja` and `ant run`.

Project is avent-driven programming pattern, where events are triggered by the motes (wireless sensor nodes).


## Spark

1. Compute a 7-day moving average of participants' age per nationality, computed for each day.
2. Calculate the percentage increase (with respect to the day before) of the 7-day moving average for each nationality.
3. Determine the top nationalities with the highest percentage increase of the seven days moving average for each day.

Have to compute once a day -> Spark SQL, batch processing. To run in distributed setting:

`export SPARK_MASTER_HOST=127.0.0.1`

If first time running, go to where spark is installed, in my case: `/Users/Carlo/Documents/spark-3.3.0-bin-hadoop2` and cd into `conf`.

Then execute: `cp spark-defaults.conf.template spark-defaults.conf`.

Make sure that spark-defaults.conf has the following lines uncommented `spark.eventLog.enabled` and `spark.eventLog.dir`

To start master: `./sbin/start-master.sh`

To start one worker: `./sbin/start-worker.sh spark://127.0.0.1:7007`

To start visualization tool: `./sbin/start-history-server.sh`

Access UI at `http://127.0.0.1:8080/`, one should see 1 worker ready.

Going pack to code repo, run `mvn package` which creates a `.jar` file inside the `target` folder.

Lastly, run: `./bin/spark-submit --class it.polimi.middleware.spark.batch.wordcount.WordCount /Users/Carlo/Desktop/POLITECNICO/NSDS/Lectures/apache_spark/NSDS_spark_tutorial/target/spark_tutorial-1.0.jar spark://127.0.0.1:7007 ~/Users/Carlo/Desktop/POLITECNICO/NSDS/Lectures/apache_spark/NSDS_spark_tutorial/`

Currently not working: `WARN StandaloneAppClient$ClientEndpoint: Failed to connect to master 127.0.0.1:7007`

Even though everything says that the master is up and running: `jps` and also `nano logs/spark-Carlo-org.apache.spark.deploy.master.Master-1-MacBook-Air-di-Carlo.local.out` has outputs shuch as *I have been elected leader! New state: ALIVE: Confirms that the Spark Master has been elected as the leader and is in the ALIVE state, indicating that it's operational.*