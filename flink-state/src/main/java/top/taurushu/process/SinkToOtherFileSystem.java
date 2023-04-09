package top.taurushu.process;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import top.taurushu.pojo.Event;

import java.util.ArrayList;
import java.util.List;

public class SinkToOtherFileSystem implements SinkFunction<Event>, CheckpointedFunction {

    private int count;
    /* SinkFunction OverWrite */
    private final int maxWriteNumber;

    private List<Event> bufferedElementLists;

    private ListState<Event> stateElementLists;


    public SinkToOtherFileSystem(int maxWriteNumber) {
        this.maxWriteNumber = maxWriteNumber;
        this.bufferedElementLists = new ArrayList<>();
    }

    @Override
    public void invoke(Event value, Context context) {
        bufferedElementLists.add(value);
        if (bufferedElementLists.size() > maxWriteNumber) {
//            bufferedElementLists.forEach(System.out::println);
            System.out.println("------------------" + count + "--------------------------------");
            count++;
            bufferedElementLists = new ArrayList<>();
        }
    }

    /* CheckPointedFunction OverWrite */

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        stateElementLists.clear();
        for (Event bufferedElementList : bufferedElementLists) {
            stateElementLists.add(bufferedElementList);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("stateElementLists", Types.POJO(Event.class));
        stateElementLists = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) stateElementLists.get().forEach(e -> bufferedElementLists.add(e));
    }
}
