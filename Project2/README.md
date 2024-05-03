# How to run in a distributed system

1. Modify the class 'ConfigUtils' and set **ALL** the correct IP addresses

2. Modify **ONLY** the .conf file of the component you want to run (they could be more than one) by setting:
* hostname = <my_ip_address>
* seed-nodes = <ip_address_of_Operator1_host> 

3. Start **FIRST** the 'Operator1' because it's the seed-node of the cluster and then all the other operators

4. At the **END** start 'StreamProcessing' to test the system

5. To test crashes in the pipeline, replace the file 'noCrashes.txt' with 'withCrashes.txt'
