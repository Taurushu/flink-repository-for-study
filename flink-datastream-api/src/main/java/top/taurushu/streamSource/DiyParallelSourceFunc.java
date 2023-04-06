package top.taurushu.streamSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class DiyParallelSourceFunc implements ParallelSourceFunction<Event> {
    private static final Long time = Calendar.getInstance().getTime().getTime() + 20 * 1000;

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) {

        Random random = new Random();

        String[] users = {"Mary", "Lily", "Bob", "Alix"};
        String[] urls = {"./home", "./math", "./product?id=2232"};

        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    new Date().getTime()
            ));
            if (Calendar.getInstance().getTime().getTime() > time) {
                cancel();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
