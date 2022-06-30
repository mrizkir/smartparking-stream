package id.ac.sttindonesia.smartparkingstream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import id.ac.sttindonesia.smartparkingstream.services.ParkingStreamConsumer;

@SpringBootApplication(
  exclude = {
    DataSourceAutoConfiguration.class
  }    
)
public class App implements CommandLineRunner {
  @Autowired
  ParkingStreamConsumer parkingStream;
  
  public static void main(String[] args) {
    SpringApplication.run(App.class, args);
  }

  @Override
  public void run(String... args) throws Exception {
    parkingStream.consume();
  }
}
