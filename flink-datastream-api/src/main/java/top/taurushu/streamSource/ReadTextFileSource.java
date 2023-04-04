package top.taurushu.streamSource;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadTextFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<String> textFile = env.readTextFile("input/UserEvent.log");
//        DataStreamSource<String> socketTextStream = env.socketTextStream("node1", 17788);

        // 封装处理逻辑
        SingleOutputStreamOperator<Event> map = textFile
                .map(s -> new Event(s.split(",")[0], s.split(",")[1], Long.valueOf(s.split(",")[2])));

        map.print();
        env.execute();
    }
}
