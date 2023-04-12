package top.taurushu;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.taurushu.pojo.Event;
import top.taurushu.util.GetKafkaSource;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        SingleOutputStreamOperator<Event> fromKafkaSource = GetKafkaSource.getFromKafkaSource(env);

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        String s = "CREATE TABLE table (\n" +
                "`name` String,\n" +
                "`uri` String,\n" +
                "`time` BIGINT" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-generate-topic',\n" +
                "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                "  'value.format' = ','\n" +
                ")";
        tblEnv.executeSql(s);

        tblEnv.executeSql("select * from table").print();
//
//        tblEnv.toDataStream(sqlQuery).print();
//
//        env.execute();
    }
}
