package top.taurushu.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GetTopN {
    public static void main(String[] args) {
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

String createSourceSql = " " +
        "CREATE TABLE inputFileTable\n" +
        "(\n" +
        "    `name` String,\n" +
        "    `url`  String,\n" +
        "    `time` BIGINT,\n" +
        "    `time_ltz` AS TO_TIMESTAMP_LTZ(`time`, 3),\n" +
        "    WaterMark For `time_ltz` as `time_ltz` - Interval '5' second\n" +
        ") WITH (\n" +
        "      'connector' = 'filesystem',\n" +
        "      'path' = 'input/input.txt',\n" +
        "      'format' = 'csv'\n" +
        "      );";
tableEnv.executeSql(createSourceSql);

String getUv = " " +
        "SELECT `name`,\n" +
        "       COUNT(1) as cn\n" +
        "FROM inputFileTable\n" +
        "GROUP BY `name`";
Table uvCount = tableEnv.sqlQuery(getUv);


String agg = " " +
        "SELECT `name`, `cn`, `rank`\n" +
        "FROM (SELECT `name`,\n" +
        "             `cn`,\n" +
        "             ROW_NUMBER() OVER(ORDER BY `cn` desc) as `rank`\n" +
        "      FROM " + uvCount + ")\n" +
        "WHERE `rank` <= 2;";
tableEnv.executeSql(agg).print();
    }
}
