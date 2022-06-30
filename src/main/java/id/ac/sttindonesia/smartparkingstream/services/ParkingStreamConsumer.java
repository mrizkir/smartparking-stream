package id.ac.sttindonesia.smartparkingstream.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import id.ac.sttindonesia.smartparkingstream.CarParkingConfig;
import id.ac.sttindonesia.smartparkingstream.KafkaConfig;
import id.ac.sttindonesia.smartparkingstream.model.SensorModel;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

@Service
public class ParkingStreamConsumer 
{  
  Logger logger = LoggerFactory.getLogger(ParkingStreamConsumer.class);
 
  @Autowired
  KafkaConfig kafkaConfig;
  
  @Autowired
  CarParkingConfig carparkConfig;
  
  public void consume()
  {    
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig.getPropConsumers());
    consumer.subscribe(Arrays.asList("sensor-1", "sensor-2", "sensor-3", "sensor-4"));
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records)
      {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        postSensor(record.value());
      }
    }
  }
  public void postSensor(String record)
  {    
    MultiValueMap<String, String> headers = new LinkedMultiValueMap();
    Map map = new HashMap<String, String>();
    map.put("Content-Type", "application/json");
    map.put("Accept", "application/json");

    headers.setAll(map);
    
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    try
    {
      SensorModel sensor = mapper.readValue(record, SensorModel.class);
      logger.info("sensorId: " + sensor.getSensorId());
      logger.info("status: " + sensor.getData().getStatus());

      Map req_payload = new HashMap();
      req_payload.put("sensor_id", sensor.getSensorId());
      req_payload.put("label", "slot-" + sensor.getSensorId());
      req_payload.put("status", sensor.getData().getStatus());

      HttpEntity<?> request = new HttpEntity<>(req_payload, headers);
      String url = carparkConfig.getApiURLv1() + "/sensor";

      ResponseEntity<?> response = new RestTemplate().postForEntity(url, request, String.class);  
      logger.info("" + response.getStatusCode());
    }
    catch (JsonProcessingException e)
    {
      logger.error(e.getMessage());
    }
    
    
  }
}

