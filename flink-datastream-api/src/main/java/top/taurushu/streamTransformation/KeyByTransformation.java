package top.taurushu.streamTransformation;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class KeyByTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> source = env.readTextFile("src/main/resources/input/flatMapText.txt");

        // 封装处理逻辑
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2KeyValue = source.flatMap(
                (String value, Collector<Tuple2<String, Long>> out) -> Arrays.stream(value.split(" ")).forEach(
                        word -> out.collect(new Tuple2<>(word, 1L))
                )
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple2KeyValue.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> keyed = tuple2StringKeyedStream.reduce(
                (Tuple2<String, Long> v1, Tuple2<String, Long> v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1)
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        keyed.keyBy(value -> "default").reduce(
                (Tuple2<String, Long> red1, Tuple2<String, Long> red2) -> red1.f1 > red2.f1 ? red1 : red2
        ).returns(Types.TUPLE(Types.STRING, Types.LONG)).print();
        env.execute();
    }
}
