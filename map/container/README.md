
Iniside container do:
// see https://kafka-tutorials.confluent.io/split-a-stream-of-events-into-substreams/kstreams.html
```
PATH=$PATH:/usr/bin/gradle/bin/
gradle wrapper
./gradlew build
./gradlew shadowJar
java -jar build/libs/kstreams-split-standalone-0.0.1.jar configuration/dev.properties
```
From Kafka container, check results:
```
kafka-console-consumer --topic facts  --bootstrap-server localhost:9092 --property print.key=true --property print.values=true
```
