package top.taurushu.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.taurushu.pojo.Event;

public class GetKafkaSource {
    private static final StreamExecutionEnvironment env;

    static {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private GetKafkaSource() {

    }


    public static SingleOutputStreamOperator<Event> getFromKafkaSource() {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics("flink-generate-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source")
                .map((MapFunction<String, Event>) Event::new).setParallelism(6);
    }

    public static void execute() throws Exception {
        env.execute();
    }
}
