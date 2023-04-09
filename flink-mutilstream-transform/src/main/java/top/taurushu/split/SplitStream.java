package top.taurushu.split;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import top.taurushu.pojo.Event;
import top.taurushu.util.FromKafkaSource;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

public class SplitStream {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {

            stream = stream.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                            Duration.ofMillis(50)
                    ).withTimestampAssigner((element, recordTimestamp) -> element.getTime())
            ).returns(Types.POJO(Event.class));
            OutputTag<Event> outMarry = new OutputTag<>("outMary", Types.POJO(Event.class));
            OutputTag<Event> outBob = new OutputTag<>("outBob", Types.POJO(Event.class));
            SingleOutputStreamOperator<Event> process = stream.process(new ProcessFunction<Event, Event>() {
                @Override
                public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx,
                                           Collector<Event> out) {
                    if (Objects.equals(value.getName().substring(0, 4), "Mary")) {
                        ctx.output(outMarry, value);
                    } else if (Objects.equals(value.getName().substring(0, 3), "Bob")) {
                        ctx.output(outBob, value);
                    } else {
                        out.collect(value);
                    }
                }
            }).returns(Types.POJO(Event.class));
            process.getSideOutput(outMarry).print("outMarry");
            process.getSideOutput(outBob).print("outBob");
            process.print("outElse");
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
