package top.taurushu.basic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.taurushu.pojo.Event;
import top.taurushu.util.GetKafkaSource;

import java.time.Duration;

public class DataStreamToTableApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> fromKafkaSource = GetKafkaSource.getFromKafkaSource(env);

        SingleOutputStreamOperator<Event> streamOperator = fromKafkaSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(50))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTime())
        );

        // 根据Stream流执行环境，创建Table Api执行环境
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        // 根据Stream流，转换为Table进行操作
        Table table = tblEnv.fromDataStream(streamOperator);


        // 根据sql进行执行
        String sql = "select `name`, `uri`, `time` from " + table;
        System.out.println(sql);

        // 使用{环境}.sqlQuery({sql})执行查询语句
        Table sqlQuery = tblEnv.sqlQuery(sql);

        // 打印表信息
        sqlQuery.printSchema();

        // 转换为DataStream Api进行打印
        tblEnv.toDataStream(sqlQuery).print("result");

        // 开始执行
        env.execute();
    }
}
