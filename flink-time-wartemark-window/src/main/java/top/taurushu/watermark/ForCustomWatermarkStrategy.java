package top.taurushu.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import top.taurushu.pojo.Event;
import top.taurushu.utils.FromKafkaSource;

import java.util.function.Function;

public class ForCustomWatermarkStrategy {
    public static void main(String[] args) throws Exception {

        Function<SingleOutputStreamOperator<Event>, Void> function = kafkaSource -> {
            kafkaSource.assignTimestampsAndWatermarks(
                    (WatermarkStrategy<Event>) context -> new WatermarkGenerator<Event>() {
                        private long maxTimestamp;

                        @Override
                        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                            maxTimestamp = Math.max(maxTimestamp, event.getTime());
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {
                            output.emitWatermark(new Watermark(maxTimestamp - 120 - 1));
                        }
                    }
            ).print();
            return null;
        };
        FromKafkaSource.executeFromKafkaSource(function);
    }
}
