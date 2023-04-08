package top.taurushu.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.function.Function;

public class ForWindowFunction {
    public static void main(String[] args) throws Exception {

        Function<SingleOutputStreamOperator<Event>, Void> function = kafkaSource -> {
            kafkaSource.assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(120)).withTimestampAssigner(
                                    (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                            )
                    )
                    .keyBy(key -> "default")
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                    .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<Long, HashSet<String>> createAccumulator() {
                            return Tuple2.of(0L, new HashSet<>());
                        }

                        @Override
                        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
                            accumulator.f1.add(value.getName());
                            return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
                        }

                        @Override
                        public Tuple2<String, String> getResult(Tuple2<Long, HashSet<String>> accumulator) {
                            return Tuple2.of(accumulator.f0.toString(), String.valueOf(accumulator.f1.size()));
                        }

                        @Override
                        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
                            a.f1.addAll(b.f1);
                            return Tuple2.of(a.f0 + b.f0, a.f1);
                        }
                    })
                    .print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
