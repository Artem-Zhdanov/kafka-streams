package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class KafkaProducerApplication {

    private final Producer<String, String> producer;
    final String outTopic;

    public KafkaProducerApplication(final Producer<String, String> producer,
                                    final String topic) {
        this.producer = producer;
        outTopic = topic;
    }

    public Future<RecordMetadata> produce(final String message) {           
        final String[] parts = message.split("-");                          
        final String key, value;                                            
        if (parts.length > 1) {                                             
          key = parts[0];                                                   
          value = parts[1];                                                 
        } else {                                                                                         
            throw new IllegalArgumentException( "NO-KEY");                                               
        }                                                                                                
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outTopic, key, value);
        return producer.send(producerRecord);                                                            
    }                                                                                                    
                                                                                                         
                                                                                                         
    public void shutdown() {                                                                             
        producer.close();                                                                                
    }                                                                                                    
                                                                                                         
    public static Properties loadProperties(String fileName) throws IOException {                        
        final Properties envProps = new Properties();                                                    
        final FileInputStream input = new FileInputStream(fileName);                                     
        envProps.load(input);                                                                            
        input.close();                                                                                   
                                                                                                         
        return envProps;                                                                                 
    }                                                                                                    
                                                                                                         
//    public static int getRandomNumber(int min, int max) {                                              
//            return (int) ((Math.random() * (max - min)) + min);                                        
//    }                                                                                                  
                                
    public void printMetadata(final Collection<Future<RecordMetadata>> metadata) {                       
        metadata.forEach(m -> {                                                                          
            try {                                                                                        
                final RecordMetadata recordMetadata = m.get();                                           
                System.out.println("Record written to offset " + recordMetadata.offset() + " timestamp " + recordMetadata.timestamp());
            } catch (InterruptedException | ExecutionException e) {                                                                    
                if (e instanceof InterruptedException) {                                                                               
                    Thread.currentThread().interrupt();                                                                                
                }                                                                                                                      
            }                                                                                                                          
        });                                                                                                                            
    }                                                                                                                                  
                                                                                                                                       
    public static void main(String[] args) throws Exception {                                                                          
        if (args.length < 1) {                                                                                                         
            throw new IllegalArgumentException(                                                                                        
                    "This program takes one argument: the path to an environment configuration file and"                               
            );                                                                                                                         
        }                                                                                                                              
                                                                                                                                       
        final Properties props = KafkaProducerApplication.loadProperties(args[0]);                                                     
        final String topic = props.getProperty("ktable.topic.name");                                                                   
        final Producer<String, String> producer = new KafkaProducer<>(props);                                                          
        final KafkaProducerApplication producerApp = new KafkaProducerApplication(producer, topic);                                    
                                                                                                                                       
            List<String> linesToProduce = new ArrayList<>();                                                                           
            for (int i = 5; i < 10000; i++) {                                                                                          
                String value = String.valueOf(i);                                                                                      
                String key = "0:" + String.format("%-64s", value).replace(' ', '0');                                                   
                System.out.println(key + "-" + value);                                                                                 
                linesToProduce.add( key + "-" + value );                                                                               
            }                                                                                
                        List<Future<RecordMetadata>> metadata = linesToProduce.stream()
                    .filter(l -> !l.trim().isEmpty())
                    .map(producerApp::produce)
                    .collect(Collectors.toList());

            producerApp.printMetadata(metadata);

            producerApp.shutdown();
    }
}
