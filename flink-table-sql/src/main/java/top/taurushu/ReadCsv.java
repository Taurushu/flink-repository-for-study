package top.taurushu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadCsv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        String s1 = "CREATE TABLE KafkaTable (\n" +
                "`name` String,\n" +
                "`url` String,\n" +
                "`time` BIGINT," +
                "`time_ltz` AS TO_TIMESTAMP_LTZ(`time`, 3)," +
                "WaterMark For `time_ltz` as `time_ltz` - Interval '1' second" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='input/input.txt',\n" +
                "  'format'='csv'\n" +
                ")";
        envTable.executeSql(s1);
//        RANGE BETWEEN INTERVAL '20' MINUTES AND CURRENT ROW
        String GetUv = " " +
                "SELECT `name`,\n" +
                "       COUNT(1) as cn\n" +
                "FROM KafkaTable\n" +
                "GROUP BY `name`";

        envTable.sqlQuery(GetUv);


        String windowSql = " " +
                "SELECT `base`.name,\n" +
                "       `base`.cn,\n" +
                "       ROW_NUMBER() OVER(PARTITION BY `base`.`name` ORDER BY `base`.`cn`) as `rank`\n" +
                "FROM () as `base`;";

        envTable.executeSql(windowSql).print();
    }
}
