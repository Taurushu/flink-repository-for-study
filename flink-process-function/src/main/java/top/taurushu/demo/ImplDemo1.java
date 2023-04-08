package top.taurushu.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import top.taurushu.pojo.Event;
import top.taurushu.pojo.UrlCount;
import top.taurushu.utils.FromKafkaSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;

public class ImplDemo1 {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {


            DataStream<UrlCount> aggregated = stream.assignTimestampsAndWatermarks(
                            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(50)).withTimestampAssigner(
                                    (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                            ))
                    .keyBy(Event::getUri)
                    .window(SlidingEventTimeWindows.of(Time.milliseconds(4000), Time.milliseconds(2000)))
                    .aggregate(new AggregateFunction<Event, UrlCount, UrlCount>() {
                                   @Override
                                   public UrlCount createAccumulator() {
                                       UrlCount urlCount = new UrlCount();
                                       urlCount.setUrl("");
                                       urlCount.setCount(0L);
                                       urlCount.setWindowStart(Long.MIN_VALUE);
                                       urlCount.setWindowEnd(Long.MIN_VALUE);
                                       return urlCount;
                                   }

                                   @Override
                                   public UrlCount add(Event value, UrlCount accumulator) {
                                       accumulator.setUrl(value.getUri());
                                       accumulator.setCount(accumulator.getCount() + 1L);
                                       return accumulator;
                                   }

                                   @Override
                                   public UrlCount getResult(UrlCount accumulator) {
                                       return accumulator;
                                   }

                                   @Override
                                   public UrlCount merge(UrlCount a, UrlCount b) {
                                       UrlCount urlCount = new UrlCount();
                                       urlCount.setUrl(a.getUrl());
                                       urlCount.setCount(a.getCount() + b.getCount());
                                       urlCount.setWindowStart(Math.min(a.getWindowStart(), b.getWindowStart()));
                                       urlCount.setWindowEnd(Math.max(a.getWindowEnd(), b.getWindowEnd()));
                                       return urlCount;
                                   }
                               },
                            new ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>() {
                                @Override
                                public void process(String s, ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>.Context context, Iterable<UrlCount> elements, Collector<UrlCount> out) {
                                    for (UrlCount element : elements) {
                                        element.setWindowStart(context.window().getStart());
                                        element.setWindowEnd(context.window().getEnd());
                                        out.collect(element);
                                        break;
                                    }

                                }
                            }
                    );
//            aggregated.print("urlCount");

            aggregated.keyBy(UrlCount::getWindowEnd)
                    .process(new TopNProcessResult(3)).print("----------------------------");

            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCount, String> {

        private Integer n;
        private ListState<UrlCount> listState;


        public TopNProcessResult() {
        }

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        public Integer getN() {
            return n;
        }

        public void setN(Integer n) {
            this.n = n;
        }

        public static void getView(ArrayList<UrlCount> elements, int number, Collector<String> out,
                                   KeyedProcessFunction<Long, UrlCount, String>.Context ctx) {
            StringBuilder builder = new StringBuilder();
            builder.append("-----------------------------------------------\n");
            builder.append("窗口结束时间: ")
                    .append(new Timestamp(ctx.getCurrentKey()))
                    .append('\n');
            for (int i = 0; i < Math.min(number, elements.size()); i++) {
                UrlCount element = elements.get(i);
                builder.append("No.").append(i + 1)
                        .append("\turl:'").append(element.getUrl())
                        .append("'\t访问量:").append(element.getCount());
                builder.append('\n');
            }

            builder.append("-----------------------------------------------\n");
            out.collect(builder.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            listState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("urlCountList", Types.POJO(UrlCount.class)));
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlCount> urlCounts = new ArrayList<>();
            listState.get().forEach(urlCounts::add);
            urlCounts.sort((o1, o2) -> (int) (o2.getCount() - o1.getCount()));
            getView(urlCounts, 3, out, ctx);
        }

        @Override
        public void processElement(UrlCount value, KeyedProcessFunction<Long, UrlCount, String>.Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 3001L);
        }
    }
}






























