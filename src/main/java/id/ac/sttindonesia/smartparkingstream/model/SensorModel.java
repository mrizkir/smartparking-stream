package id.ac.sttindonesia.smartparkingstream.model;

public class SensorModel {
  private String sensorId;

  private PropertySensor data;

  public String getSensorId() {
    return sensorId;
  }

  public void setSensorId(String sensorId) {
    this.sensorId = sensorId;
  }  
  
  public PropertySensor getData() {
    return data;
  }

  public void setData(PropertySensor data) {
    this.data = data;
  }
}
