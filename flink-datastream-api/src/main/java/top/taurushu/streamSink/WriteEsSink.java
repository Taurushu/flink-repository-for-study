package top.taurushu.streamSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

public class WriteEsSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<String> returns = kafkaSource.map("舒杰"::concat).returns(Types.STRING);

        returns.sinkTo(
                new Elasticsearch7SinkBuilder<String>()
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost("192.168.32.151", 9200, "http"))
                        .setEmitter(
                                (element, context, indexer) ->
                                        indexer.add(createIndexRequest(element)))
                        .build());

        env.execute();
    }

    private static IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .id(element)
                .source(json);
    }
}
