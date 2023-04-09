package top.taurushu.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;


public class RunBroadcastStateStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Action> actionS = env.fromElements(
                new Action("Mary", "login"),
                new Action("Bob", "login"),
                new Action("Mary", "login"),
                new Action("Mary", "cart"),
                new Action("Mary", "pay"),
                new Action("Bob", "cart"),
                new Action("Bob", "pay")
        );
        DataStreamSource<Pattern> patternS = env.fromElements(
                new Pattern("login", "cart"),
                new Pattern("cart", "pay")
        );
        MapStateDescriptor<Void, Pattern> patternDescriptor
                = new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcast = patternS.broadcast(patternDescriptor);

        actionS.keyBy(Action::getUserId, Types.STRING).connect(broadcast).process(new PatternAction()).print();


        env.execute();
    }

    public static class PatternAction extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple4<String, Action, Action, Pattern>> {

        ValueState<Action> patternValueAction;


        @Override
        public void open(Configuration parameters) {
            patternValueAction = getRuntimeContext().getState(new ValueStateDescriptor<>("patternValueState", Action.class));
        }

        @Override
        public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple4<String, Action, Action, Pattern>>.ReadOnlyContext ctx, Collector<Tuple4<String, Action, Action, Pattern>> out) throws Exception {
            if (Objects.isNull(patternValueAction.value())) {
                patternValueAction.update(value);
                return;
            }

            Action lastValue = patternValueAction.value();
            Pattern pattern = ctx.getBroadcastState(new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class))).get(null);

            if (!Objects.isNull(value) && !Objects.isNull(pattern) && Objects.equals(value.action, pattern.actionNext)
                    && Objects.equals(lastValue.action, pattern.actionFirst)) {
                out.collect(Tuple4.of(value.userId, lastValue, value, pattern));
            }

            patternValueAction.update(value);
        }

        @Override
        public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple4<String, Action, Action, Pattern>>.Context ctx, Collector<Tuple4<String, Action, Action, Pattern>> out) throws Exception {
            ctx.getBroadcastState(
                    new MapStateDescriptor<>("pattern-state", Types.VOID, Types.POJO(Pattern.class))
            ).put(null, value);
        }
    }


    public static class Action {
        private String userId;
        private String action;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        private String actionFirst;
        private String actionNext;

        public Pattern() {
        }

        public Pattern(String actionFirst, String actionNext) {
            this.actionFirst = actionFirst;
            this.actionNext = actionNext;
        }

        public String getActionFirst() {
            return actionFirst;
        }

        public void setActionFirst(String actionFirst) {
            this.actionFirst = actionFirst;
        }

        public String getActionNext() {
            return actionNext;
        }

        public void setActionNext(String actionNext) {
            this.actionNext = actionNext;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "actionFirst='" + actionFirst + '\'' +
                    ", actionNext='" + actionNext + '\'' +
                    '}';
        }
    }

}
