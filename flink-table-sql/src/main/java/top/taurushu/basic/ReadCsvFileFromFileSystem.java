package top.taurushu.basic;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadCsvFileFromFileSystem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        String s1 = "CREATE TABLE KafkaTable (\n" +
                "`name` String,\n" +
                "`url` String,\n" +
                "`time` BIGINT" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/input.txt',\n" +
                "  'format'='csv'\n" +
                ")";

        envTable.executeSql(s1);

        envTable.executeSql("select `name`,`url`, `time` from KafkaTable").print();

    }
}
