package top.taurushu.union;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import top.taurushu.pojo.Pay;
import top.taurushu.pojo.ThirdPayPlatform;

import java.time.Duration;
import java.util.Objects;

public class ElseConnectStream {
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


        paySource.connect(thirdSource)
                .keyBy(Pay::getOrderId, ThirdPayPlatform::getOrderId)
                .process(new CoProcessFunction<Pay, ThirdPayPlatform, String>() {
                    ValueState<Pay> payValueState;
                    ValueState<ThirdPayPlatform> platformValueState;

                    @Override
                    public void open(Configuration parameters) {
                        payValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pay-event", Types.POJO(Pay.class))
                        );
                        platformValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("third-event", Types.POJO(ThirdPayPlatform.class))
                        );
                    }

                    @Override
                    public void processElement1(Pay value, CoProcessFunction<Pay, ThirdPayPlatform, String>.Context ctx, Collector<String> out) throws Exception {
                        if (Objects.isNull(platformValueState.value())) {
                            payValueState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.getTimestampLong() + 5000L);
                        } else {
                            out.collect("对账成功\t" + value.toString() + "\t" + platformValueState.value());
                            platformValueState.clear();
                        }
                    }

                    @Override
                    public void processElement2(ThirdPayPlatform value, CoProcessFunction<Pay, ThirdPayPlatform, String>.Context ctx, Collector<String> out) throws Exception {
                        if (Objects.isNull(payValueState.value())) {
                            platformValueState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.getTime() + 5000L);
                        } else {
                            out.collect("对账成功\t" + payValueState.value() + "\t" + value.toString());
                            payValueState.clear();
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<Pay, ThirdPayPlatform, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        if (!Objects.isNull(payValueState)) {
                            out.collect("对账失败\t" + payValueState.value() + "\t" + "NUll");
                        }
                        if (!Objects.isNull(platformValueState)) {
                            out.collect("对账失败\t" + "NUll" + "\t" + platformValueState.value());
                        }
                        payValueState.clear();
                        platformValueState.clear();
                    }
                }).print();

        env.execute();
    }
}
