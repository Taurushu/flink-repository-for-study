package top.taurushu.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.util.function.Function;

public class ForMonotonousTimestampsStrategy {
    public static void main(String[] args) throws Exception {

        Function<SingleOutputStreamOperator<Event>, Void> function = kafkaSource -> {
            kafkaSource.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(
                            (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTime()
                    )
            ).print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
