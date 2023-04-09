package top.taurushu.union;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ints = env.fromElements(1, 2, 3);
        DataStreamSource<Long> longs = env.fromElements(1L, 2L, 3L);

        ints.connect(longs).map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) {
                return value.toString();
            }

            @Override
            public String map2(Long value) {
                return value.toString();
            }
        }).print();



        env.execute();
    }
}
