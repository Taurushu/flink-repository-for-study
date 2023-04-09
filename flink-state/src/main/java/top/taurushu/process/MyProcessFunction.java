package top.taurushu.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.taurushu.pojo.Event;

import java.util.Objects;

public class MyProcessFunction extends KeyedProcessFunction<String, Event, String> {

    ValueState<Integer> timesState;
    ValueState<Long> lastTimestamp;
    ValueState<Long> sumIntelState;

    @Override
    public void open(Configuration parameters) {
        timesState = getRuntimeContext().getState(new ValueStateDescriptor<>("timesStateState", Integer.class));
        sumIntelState = getRuntimeContext().getState(new ValueStateDescriptor<>("sumIntelStateState", Long.class));
        lastTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTimestampState", Long.class));
    }

    @Override
    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
        if (Objects.isNull(lastTimestamp.value()) || lastTimestamp.value() == 0) {
            lastTimestamp.update(value.getTime());
            timesState.update(1);
            sumIntelState.update(0L);
        } else {
            sumIntelState.update(sumIntelState.value() + Math.abs(value.getTime() - lastTimestamp.value()) / 2);
            lastTimestamp.update(value.getTime());
            timesState.update(timesState.value() + 1);
            out.collect(ctx.getCurrentKey() + " 的平均访问间隔是：" + sumIntelState.value() / timesState.value());
        }
    }
}
