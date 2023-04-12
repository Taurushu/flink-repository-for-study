package top.taurushu.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.collections.CollectionUtils.collect;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class GetTopN_2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporarySystemFunction("MyFunction", MyFunction.class);
        tableEnv.createTemporarySystemFunction("MyTableAggregateFunction", MyTableAggregateFunction.class);
//        tableEnv.createTemporarySystemFunction("MyAggregateFunction", MyTableAggregateFunction.class);

        String createSourceSql = " " +
                "CREATE TABLE inputFileTable\n" +
                "(\n" +
                "    `name` String,\n" +
                "    `url`  String,\n" +
                "    `time` BIGINT,\n" +
                "    `time_ltz` AS TO_TIMESTAMP_LTZ(`time`, 3),\n" +
                "    WaterMark For `time_ltz` as `time_ltz` - Interval '1' SECONDS\n" +
                ") WITH (\n" +
                "      'connector' = 'filesystem',\n" +
                "      'path' = 'input/input.txt',\n" +
                "      'format' = 'csv'\n" +
                "      );";
        tableEnv.executeSql(createSourceSql);

        String getUv = " " +
                "SELECT `name`,\n" +
                "       window_start,\n" +
                "       window_end,\n" +
                "       COUNT(1) as cn\n" +
                "FROM TABLE(TUMBLE(TABLE inputFileTable, DESCRIPTOR(`time_ltz`), INTERVAL '1' SECONDS))\n" +
                "GROUP BY `name`, window_start, window_end;";
        Table uvCount = tableEnv.sqlQuery(getUv);

        String agg = " " +
                "SELECT MyFunction(`name`) as `name`, `cn`, window_start, window_end, `rank`\n" +
                "FROM (SELECT `name`,\n" +
                "             `cn`,\n" +
                "             window_start, " +
                "             window_end, " +
                "             ROW_NUMBER() OVER(PARTITION BY window_start, window_end ORDER BY `cn` desc) as `rank`\n" +
                "      FROM " + uvCount + ")\n" +
                "WHERE `rank` <= 2;";
        Table sqlQuery = tableEnv.sqlQuery(agg);
        Table select = sqlQuery.groupBy($("window_end")).flatAggregate(call("MyTableAggregateFunction", $("name")).as("value"))
                .select($("window_end"), $("value"));

        select.execute().print();
//        tableEnv.executeSql("select MyTableAggregateFunction(name) from " + sqlQuery).print();
    }

//    @FunctionHint(output = @DataTypeHint("ROW<word String, length Int>"))
    public static class MyTableAggregateFunction extends TableAggregateFunction<String, List<String>>  {

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }

        public void accumulate(List<String> list, String value) {
            list.add(value);
        }

        public void emitValue(List<String> list, Collector<String> out) {
            list.forEach(out::collect);
        }

    }



    public static class MyFunction extends ScalarFunction {
        public String eval(String s) {

            return " --" + s + "-- ";
        }
    }

    @FunctionHint(output = @DataTypeHint("ROW<word String, length Int>"))
    public static class MyTableFunction extends TableFunction<Row> {
        public void eval(String s) {
            for (int i = 1; i <= 3; i++) {
                collect(Row.of(String.valueOf(i), i));
            }
        }
    }

    public static class MyAggregateFunction extends AggregateFunction<String, List<String>>  {

        public void accumulate(List<String> list, String value) {
            list.add(value);
        }

        @Override
        public String getValue(List<String> list) {
            StringBuilder builder = new StringBuilder();
            list.forEach(builder::append);
            return builder.toString();
        }

        @Override
        public List<String> createAccumulator() {
            return new ArrayList<>();
        }
    }
}
