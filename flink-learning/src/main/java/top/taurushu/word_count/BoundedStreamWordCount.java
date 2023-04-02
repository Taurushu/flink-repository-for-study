package top.taurushu.word_count;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.获取数据源、读取数据
        DataStreamSource<String> lineSource = env.readTextFile("input/");

        // 3.通过lambda表达式，将lineSource转换成Tuple2格式然后收集起来，扁平映射出来
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2Return = lineSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] str = line.split(" ");
                    for (String s : str) {
                        out.collect(Tuple2.of(s, 1L));
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.通过Tuple中的 `0号元素(key)` 对 `1号元素(value)` 进行Sum聚合统计
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2Return.keyBy(data -> data.f0).sum(1);
        // 5.打印出结果
        sum.print();

        env.execute();
    }
}
