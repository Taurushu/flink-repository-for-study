package top.taurushu.streamSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Date;
import java.util.Random;

public class ReadDIYSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> source = env.addSource(new DiyParallelSourceFunc()).setParallelism(2);

        source.print();

        env.execute();
    }
}
