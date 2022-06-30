package id.ac.sttindonesia.smartparkingstream;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties
public class KafkaConfig {    
    @Value("${kafka.bootstrap-servers}")
    private String BootstrapServers;
    
    @Value("${kafka.consumer.group-id}")
    private String ConsumerGroupID;
    
    @Value("${kafka.consumer.auto-offset-reset}")
    private String AutoOffsetReset;

    public String getBootstrapServers() {
      return BootstrapServers;
    }

    public void setBootstrapServers(String BootstrapServers) {
      this.BootstrapServers = BootstrapServers;
    }
    public String getConsumerGroupID() {
      return ConsumerGroupID;
    }

    public void setConsumerGroupID(String ConsumerGroupID) {
      this.ConsumerGroupID = ConsumerGroupID;
    }
    public String getAutoOffsetReset() {
      return AutoOffsetReset;
    }

    public void setAutoOffsetReset(String AutoOffsetReset) {
      this.AutoOffsetReset = AutoOffsetReset;
    }
    /**
     * digunakan untuk mendapatkan properties untuk kafka producers atau consumers
     * @return properties
    */
    public Properties getPropProducers()
    {
      Properties prop = new Properties();
      prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
      prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());        
      prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
      return prop;
    }
    /**
     * digunakan untuk mendapatkan properties untuk kafka producers atau consumers
     * @return properties
    */
    public Properties getPropConsumers()
    {
      Properties prop = new Properties();
      prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
      prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());        
      prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); 
      prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getConsumerGroupID()); 
      prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.getAutoOffsetReset());  
      return prop;
    }
}
