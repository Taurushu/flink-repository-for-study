package top.taurushu.streamTransformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.taurushu.streamSource.Event;

public class FilterTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textFile = env.readTextFile("src\\main\\resources\\input\\UserEvent.log");

        // 封装处理逻辑
        SingleOutputStreamOperator<Event> map = textFile.map(
                s -> new Event(s.split(",")[0], s.split(",")[1], Long.valueOf(s.split(",")[2]))
        ).returns(Types.POJO(Event.class));
        map = map.filter(value -> !"Bob".equals(value.getName()));

        map.print();

        env.execute();
    }
}
