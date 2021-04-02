package io.confluent.developer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.GlobalKTable;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// import io.confluent.developer.avro.ActingEvent;
// import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class SplitStream {

    public Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));        
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));  
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());           
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,  Serdes.String().getClass());        
        return props;                                                                                  
    }                                                                                                  
                                                                                                       
                                                                                                     
    public int getRandomNumber(int min, int max) {                                                     
            return (int) ((Math.random() * (max - min)) + min);                                      
    }                                                                                                  
                                                                                                       
    public Topology buildTopology(Properties envProps) {                                             
        final StreamsBuilder builder = new StreamsBuilder();                                         
        final MovieRatingJoiner joiner = new MovieRatingJoiner();                                    
                                                                                               
                                                                                                       
        GlobalKTable<String, String> ktable = builder.globalTable(envProps.getProperty("ktable.topic.name"));
                                                                                                             
                                                                                                             
        builder.stream(envProps.getProperty("input.topic.name"))                                             
                .map((k, v) -> {                                                                             
                        String key = "0:" + String.format("%-64s", String.valueOf( getRandomNumber( 0, 50000 ))).replace(' ', '0');
                        JSONObject jo = new JSONObject((String) v);                                                                
                        return new KeyValue<>(key, jo.getString("id"));                                                            
                })                                                                                                                 
                .join(ktable, joiner)                                                                                              
                .to(envProps.getProperty("output.topic.name"));                                                                    
                                                                                                                                   
        return builder.build();                                                                                                    
    }                                                                
       public void createTopics(Properties envProps) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
        AdminClient client = AdminClient.create(config);

        List<NewTopic> topics = new ArrayList<>();

        topics.add(
               (new NewTopic(
                        envProps.getProperty("output.topic.name"),
                        Integer.parseInt(envProps.getProperty("output.topic.partitions")),
                        Short.parseShort(envProps.getProperty("output.topic.replication.factor"))
                )).configs(
                        new HashMap<String, String>() {{
                                put("cleanup.policy", "delete" );
                                put("retention.ms", "300000"   );
                                put("retention.bytes", "-1"  );
                }})
        );

        topics.add(
               (new NewTopic(
                        envProps.getProperty("ktable.topic.name"),
                        Integer.parseInt(envProps.getProperty("ktable.topic.partitions")),
                        Short.parseShort(envProps.getProperty("ktable.topic.replication.factor"))
                )).configs(
                        new HashMap<String, String>() {{
                                put("cleanup.policy", "delete" );
                                put("retention.ms", "300000"   );
                                put("retention.bytes", "-1"  );
                }})
        );

        client.createTopics(topics);
                client.close();
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        SplitStream ss = new SplitStream();
        Properties envProps = ss.loadEnvProperties(args[0]);
        Properties streamProps = ss.buildStreamsProperties(envProps);
        ss.createTopics(envProps);

        Topology topology = ss.buildTopology(envProps);


        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);
                // Attach shutdown handler to catch Control-C.
        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

