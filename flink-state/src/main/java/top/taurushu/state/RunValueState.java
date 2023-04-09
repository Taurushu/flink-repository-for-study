package top.taurushu.state;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.process.MyProcessFunction;
import top.taurushu.util.FromKafkaSource;

import java.util.function.Function;

public class RunValueState {
    public static void main(String[] args) throws Exception {
        Function<SingleOutputStreamOperator<Event>, Void> function = (SingleOutputStreamOperator<Event> stream) -> {
            stream.keyBy(Event::getName)
                    .process(new MyProcessFunction())
                    .print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
