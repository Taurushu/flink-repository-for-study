package top.taurushu.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ImplDemo2 {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {
            stream.assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<Event>forBoundedOutOfOrderness(Duration.ofMillis(50))
                                    .withTimestampAssigner((Event element, long recordTimestamp) ->
                                            element.getTime()
                                    )
                    )
                    .map(Event::getUri)

                    .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(1000), Time.milliseconds(500)))
                    .aggregate(new AggregateFunction<String, Map<String, Long>, List<Tuple2<String, Long>>>() {
                        @Override
                        public Map<String, Long> createAccumulator() {
                            return new HashMap<>();
                        }

                        @Override
                        public Map<String, Long> add(String value, Map<String, Long> accumulator) {
                            if (accumulator.containsKey(value)) {
                                accumulator.put(value, accumulator.get(value) + 1L);
                            } else {
                                accumulator.put(value, 1L);
                            }
                            return accumulator;
                        }

                        @Override
                        public List<Tuple2<String, Long>> getResult(Map<String, Long> accumulator) {
                            List<Map.Entry<String, Long>> list = new ArrayList<>(accumulator.entrySet());
                            list.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));

                            ArrayList<Tuple2<String, Long>> returnList = new ArrayList<>();
                            for (int i = 0; i < 3; i++) {
                                if (list.size() > i) {
                                    returnList.add(Tuple2.of(list.get(i).getKey(), list.get(i).getValue()));
                                }
                            }
                            return returnList;
                        }

                        @Override
                        public Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
                            HashMap<String, Long> map = new HashMap<>();
                            map.putAll(a);
                            map.putAll(b);
                            return map;
                        }
                    }
                    , new ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow>() {
                        @Override
                        public void process(ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow>.Context context,
                                            Iterable<List<Tuple2<String, Long>>> elements,
                                            Collector<String> out) {
                            StringBuilder builder = new StringBuilder();
                            builder.append("-----------------------------------------------\n");
                            for (List<Tuple2<String, Long>> element : elements) {
                                builder.append("窗口结束时间: ")
                                        .append(new Timestamp(context.window().getEnd()))
                                        .append('\n');
                                for (int i = 0; i < element.size(); i++) {
                                    Tuple2<String, Long> tuple2 = element.get(i);
                                    builder.append("No.").append(i + 1)
                                            .append("\turl:'").append(tuple2.f0)
                                            .append("'\t访问量:").append(tuple2.f1);
                                    builder.append('\n');
                                }
                                break;
                            }
                            builder.append("-----------------------------------------------\n");
                            out.collect(builder.toString());
                        }
                    }
                    ).print();


            return null;
        };


        FromKafkaSource.executeFromKafkaSource(function);
    }
}






























