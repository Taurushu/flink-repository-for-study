package top.taurushu.basic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.taurushu.pojo.Event;
import top.taurushu.util.GetKafkaSource;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class ReadAndWriteCsvFileFromFileSystem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = GetKafkaSource.getFromKafkaSource(env)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(120))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTime())
                );

        Table table = envTable.fromDataStream(stream, Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("uri", DataTypes.STRING())
                        .column("time", DataTypes.BIGINT())
//                .columnByExpression("ts", "PROCTIME()")
                        .columnByExpression("ts", "TO_TIMESTAMP_LTZ(`time`, 3)")
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build()
        );

        envTable.createTemporaryView("table", table);

        String windowSql = " " +
                "select `name`, window_start, window_end, count(1)" +
                "from Table(Cumulate(TABLE `table`, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS)) " +
                "group by `name`, window_start, window_end;";


        envTable.executeSql(windowSql).print();
    }
}
