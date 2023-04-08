package top.taurushu.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.time.Duration;
import java.util.HashSet;
import java.util.function.Function;

public class CountUrlTop5 {
    public static void main(String[] args) throws Exception {


        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {
            OutputTag<Event> eventOutputTag = new OutputTag<Event>("eventLate", Types.POJO(Event.class));
            SingleOutputStreamOperator<String> mainOutPut = stream.assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(50))
                                    .withTimestampAssigner(
                                    (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                            )
                    ).keyBy(Event::getUri)
                    .window(SlidingProcessingTimeWindows.of(Time.milliseconds(200), Time.milliseconds(20)))
                    .sideOutputLateData(eventOutputTag)
                    .aggregate(new AggregateFunction<Event, Tuple3<String, Long, HashSet<String>>, String>() {

                        @Override
                        public Tuple3<String, Long, HashSet<String>> createAccumulator() {
                            return Tuple3.of("default", 0L, new HashSet<>());
                        }

                        @Override
                        public Tuple3<String, Long, HashSet<String>> add(
                                Event value,
                                Tuple3<String, Long, HashSet<String>> accumulator) {
                            accumulator.f2.add(value.getName());
                            return Tuple3.of(value.getUri(), accumulator.f1 + 1, accumulator.f2);
                        }

                        @Override
                        public String getResult(Tuple3<String, Long, HashSet<String>> accumulator) {
                            return accumulator.f0 +
                                    "\t" + accumulator.f1 +
                                    "\t" + accumulator.f2.size() +
                                    "\t";
                        }

                        @Override
                        public Tuple3<String, Long, HashSet<String>> merge(Tuple3<String, Long, HashSet<String>> a, Tuple3<String, Long, HashSet<String>> b) {
                            a.f2.addAll(b.f2);
                            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2);
                        }
                    })
                    .returns(Types.STRING);
            mainOutPut.getSideOutput(eventOutputTag).print("outside");
            mainOutPut.print("main");
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
