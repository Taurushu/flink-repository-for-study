package top.taurushu.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.function.Function;

public class ForAggregateFullWindowFunction {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {
            stream.assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(120)).withTimestampAssigner(
                                    (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                            )
                    )
                    .keyBy(Event::getName)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                    .aggregate(new CustomAggWinFunc(), new CustomProWinFunc())
                    .print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }

    static class CustomAggWinFunc implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.getName());
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            HashSet<String> set = new HashSet<>();
            set.addAll(a);
            set.addAll(b);
            return set;
        }
    }

    static class CustomProWinFunc extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            for (Long size : elements) {
                out.collect(new Timestamp(context.window().getStart())
                        + " ~ " + new Timestamp(context.window().getEnd())
                        + ": " + size);
                break;
            }
        }
    }
}

