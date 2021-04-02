```
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/bin/gradle/bin/
./gradlew shadowJar
java -jar build/libs/kafka-producer-application-standalone-0.0.1.jar configuration/dev.properties
```
