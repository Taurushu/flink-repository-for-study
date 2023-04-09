package top.taurushu.union;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.util.GetKafkaSource;


public class UnionStream {
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<Event> fromKafkaSource1 = GetKafkaSource.getFromKafkaSource();
        SingleOutputStreamOperator<Event> fromKafkaSource2 = GetKafkaSource.getFromKafkaSource();

        fromKafkaSource1.union(fromKafkaSource2).map((Event e) -> 1).keyBy(l -> 1).sum(0).print();


        GetKafkaSource.execute();
    }
}
