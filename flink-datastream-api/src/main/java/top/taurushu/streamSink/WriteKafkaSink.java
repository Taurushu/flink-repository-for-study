package top.taurushu.streamSink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.taurushu.streamSource.DiyParallelSourceFunc;
import top.taurushu.streamSource.Event;

public class WriteKafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new DiyParallelSourceFunc()).setParallelism(2);

        SingleOutputStreamOperator<String> returns = source.map(Event::toCsv).returns(Types.STRING);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("node1:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("flink-generate-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        returns.sinkTo(sink);
        env.execute();
    }
}
