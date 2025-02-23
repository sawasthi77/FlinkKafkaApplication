package flink_kafka_integration_demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaIntegrationReceiver {
    public static void main(String[] args) throws Exception {
        String inputTopic = "test-topic";
        String server = "localhost:9092";
        StreamConsumer(inputTopic, server);
    }

    public static void StreamConsumer(String inputTopic, String inputServer)throws Exception{
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                createStringConsumerForTopic(inputTopic, inputServer);
        DataStream<String> inputStream =
                environment.addSource(flinkKafkaConsumer);

        inputStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -999736771747691234L;
            @Override
            public String map(String value)throws Exception{
                return "Receiving from kafka: " + value;
            }
        }).print();
        environment.execute();
    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        return consumer;
    }
}
