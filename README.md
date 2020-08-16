# NYC_Taxi_streaming
The streaming data pipeline:
he streaming data will be ingested with Kafka and then Spark Structured Streaming will be leveraged to process data in near real-time. The Taxi data slightly modified for the Apache Flink Training will be used. It contains a collection of ride events with information such as the driver id, the amount paid, and an indication if the ride is starting or ending. Data is available in two compressed files: nycTaxiRides.gz with rides’ essential information including geographical localization, and nycTaxiFares.gz conveying the rides’ financial information. 

The selected use case is the identification of the Manhattan neighborhoods that are most likely to yield high tips. A cab driver with such information could favor places that recently peaked in tips to boost his earning. Note about the pertinence of this dataset, tip data is collected only from payments by card, whereas in reality tips are often given in cash. It’s a scenario entailing the use of streaming processing because the detection of the elevated tip has to be as fast as possible.



    What Manhattan neighborhoods should Taxi driver choose to get a high tip?

Based only on top 20 rows, a driver would receive a notification at 21h50 that going to Vinegar Hill for a next ride is a good idea.
![alt text](https://github.com/Roshni96/NYC_Taxi_streaming/blob/master/finalres.png)

The following steps are performed:

    Data engineering to prepare ML input
    ML model development
    Integration of the ML model within the streaming data pipeline

A Spark MLlib pipeline is trained on historic data in batch mode. Then, the resulting model will be used in a separate Spark streaming application. The computations of the clusters are supposed to be periodically relaunched to update the ML model.

The image below shows the clusters for the time segment 10 am-11 am. The visualization was obtained after loading the “clusters-mean-stddev.geojson” file on the geojson.io website.

![alt text](https://github.com/Roshni96/NYC_Taxi_streaming/blob/master/example-geojson.png)
