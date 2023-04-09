package top.taurushu.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.taurushu.pojo.Pay;
import top.taurushu.pojo.ThirdPayPlatform;

import java.time.Duration;

public class ConWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Pay> paySource = env.fromElements(
                new Pay("order_01", "app", 1000L), //  900L  750~1000   1000~1500
                new Pay("order_02", "app", 1200L), // 1100L
                new Pay("order_03", "app", 1300L), // 1200L
                new Pay("order_04", "app", 1400L), // 1300L
                new Pay("order_05", "app", 1500L)  // 1400L
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Pay>forBoundedOutOfOrderness(Duration.ofMillis(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestampLong()));


        SingleOutputStreamOperator<ThirdPayPlatform> thirdSource = env.fromElements(
                new ThirdPayPlatform("order_01", "ThirdPayPlatform", true, 1000L - 100L),
                new ThirdPayPlatform("order_02", "ThirdPayPlatform", true, 1200L - 100L),
                new ThirdPayPlatform("order_03", "ThirdPayPlatform", true, 1300L - 100L),
                new ThirdPayPlatform("order_04", "ThirdPayPlatform", true, 1400L - 100L),
                new ThirdPayPlatform("order_05", "ThirdPayPlatform", true, 1500L - 100L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<ThirdPayPlatform>forBoundedOutOfOrderness(Duration.ofMillis(100))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTime()));


        paySource.keyBy(Pay::getOrderId).coGroup(thirdSource.keyBy(ThirdPayPlatform::getOrderId))
                .where(Pay::getOrderId, Types.STRING).equalTo(ThirdPayPlatform::getOrderId, Types.STRING)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(500), Time.milliseconds(250)))
                .apply((CoGroupFunction<Pay, ThirdPayPlatform, String>) (first, second, out) -> out.collect(first + "->" + second))
                .print();
        env.execute();
    }
}
