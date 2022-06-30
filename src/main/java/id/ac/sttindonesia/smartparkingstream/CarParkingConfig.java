package id.ac.sttindonesia.smartparkingstream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties
public class CarParkingConfig {    
  @Value("${carparking.api-url-v1}")
  private String ApiURLv1;

  public String getApiURLv1() {
    return ApiURLv1;
  }
}
