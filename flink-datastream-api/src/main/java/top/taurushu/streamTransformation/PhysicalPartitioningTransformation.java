package top.taurushu.streamTransformation;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import top.taurushu.streamSource.Event;

import java.util.Arrays;


public class PhysicalPartitioningTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> source = env.addSource(new RichParallelSourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    int number = getRuntimeContext().getNumberOfParallelSubtasks();
                    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                    if (i % number == indexOfThisSubtask) {
                        ctx.collect(new Event(String.valueOf(i), "./" + i + ".html", (long) i));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2);


        source.rescale().print().setParallelism(4);

        env.execute();
    }
}
