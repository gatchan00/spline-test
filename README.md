# spline-test
A test for using spline (against mongo on localhost)

# How to launch it:
This project contains 2 clases that may be used

com.everis.gatchan.BasicExample -> Stores in HDFS the result of reading and joining a couple of csv files
com.everis.gatchan.BasicExampleKafka -> Same as above, but storing it in kafka

Both clases expect a config file for the process(like the one provided in the resource folder) and for the spline

Both mongo and kafka are meant to be at localhost. Kafka broker is hardcoded and mongo url is located at spline config file.

#How to launch spline UI

Download it from https://absaoss.github.io/spline/

Remember to change the mongo url
java -jar spline-web-0.3.9-exec-war.jar -Dspline.persistence.factory=za.co.absa.spline.persistence.mongo.MongoPersistenceFactory -Dspline.mongodb.url=mongodb://127.0.0.1:27017/gatchan -Dspline.mongodb.name=gatchan
