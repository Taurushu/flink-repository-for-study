package top.taurushu.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.taurushu.pojo.Event;

import java.util.function.Function;

public class FromKafkaSource {
    private FromKafkaSource() {

    }

    public static void executeFromKafkaSource(Function<SingleOutputStreamOperator<Event>, Void> function) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics("flink-generate-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        SingleOutputStreamOperator<Event> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka Source")
                .map(Event::new).setParallelism(6);

        function.apply(kafkaSource);
        env.execute();
    }
}
