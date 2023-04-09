package top.taurushu.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.process.SinkToOtherFileSystem;
import top.taurushu.util.FromKafkaSource;

import java.time.Duration;
import java.util.function.Function;

public class RunOperatorState {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {
            stream = stream.setParallelism(1);
            stream = stream.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(200))
                            .withTimestampAssigner((element, recordTimestamp) -> element.getTime())
            ).setParallelism(1);
            stream.addSink(new SinkToOtherFileSystem(2000)).setParallelism(1);
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
