package top.taurushu.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.time.Duration;
import java.util.function.Function;

public class ForBoundedOutOfOrdernessStrategy {
    public static void main(String[] args) throws Exception {

        Function<SingleOutputStreamOperator<Event>, Void> function = kafkaSource -> {
            kafkaSource.assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(120)).withTimestampAssigner(
                                    (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                            )
                    ).map((Event e) -> 1L)
                    .keyBy(key -> "default")
                    .countWindow(200)
//                    .window(TumblingEventTimeWindows.of(Time.milliseconds(1000),Time.milliseconds(500)))
                    .reduce((ReduceFunction<Long>) Long::sum)
                    .print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
