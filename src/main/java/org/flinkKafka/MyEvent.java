package org.flinkKafka;

public class MyEvent {
    public String field_name;  // Must match JSON field

    // Default constructor needed by Flink
    public MyEvent() {}

    public MyEvent(String field_name) {
        this.field_name = field_name;
    }
}
