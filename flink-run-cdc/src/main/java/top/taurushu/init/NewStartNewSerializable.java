package top.taurushu.init;

import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class NewStartNewSerializable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        MySqlSource<String> build = MySqlSource.<String>builder()
                .hostname("192.168.32.151")
                .port(3306)
                .username("root")
                .password("shujie")
                .databaseList("mydb")
                .tableList()  // ALL Tables
                .deserializer(new MysqlInfoSerialization())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(build, WatermarkStrategy.noWatermarks(), "mysqlSource");

        mysqlSource.print();
        env.execute();
    }

    public static class MysqlInfoSerialization implements DebeziumDeserializationSchema<String>{
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            Map<String, Object> map = new HashMap<>();
            map.put("database", sourceRecord.topic().split("\\.")[1]);
            map.put("table", sourceRecord.topic().split("\\.")[2]);

            Arrays.stream(new String[]{"before", "after"}).forEach(
                    elements -> {
                        Struct struct = ((Struct) sourceRecord.value()).getStruct(elements);
                        if (!Objects.isNull(struct)) {
                                Map<String, Object> schemaInfos = new HashMap<>();
                            struct.schema().fields().forEach(
                                    filed -> schemaInfos.put(filed.name(), struct.get(filed))
                            );
                            map.put(elements, schemaInfos);
                        } else {
                            map.put(elements, null);
                        }
                    }
            );
            map.put("op", ((Struct) sourceRecord.value()).get("op"));
            collector.collect(JSON.toJSONString(map));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
