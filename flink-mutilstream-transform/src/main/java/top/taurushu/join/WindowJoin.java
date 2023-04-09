package top.taurushu.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.taurushu.pojo.Pay;
import top.taurushu.pojo.ThirdPayPlatform;

import java.time.Duration;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Pay> paySource = env.fromElements(
                new Pay("order_01", "app", 1000L),
                new Pay("order_02", "app", 1200L),
                new Pay("order_03", "app", 1300L),
                new Pay("order_04", "app", 1400L),
                new Pay("order_05", "app", 1500L)
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

        paySource.join(thirdSource)
                .where(Pay::getOrderId).equalTo(ThirdPayPlatform::getOrderId)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(200), Time.milliseconds(100)))
                .apply((Pay first, ThirdPayPlatform second) -> first + " -> " + second)
                .print();
        env.execute();
    }
}
