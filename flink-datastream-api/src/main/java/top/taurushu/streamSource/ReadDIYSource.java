package top.taurushu.streamSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Random;

public class ReadDIYSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.addSource(new SourceFunction<Event>() {
            private Boolean running = true;

            @Override
            public void run(SourceContext<Event> sourceContext) throws Exception {

                Random random = new Random();

                String[] users = {"Mary", "Lily", "Bob", "Alix"};
                String[] urls = {"./home", "./math", "./product?id=2232"};

                while (running) {
                    sourceContext.collect(new Event(
                            users[random.nextInt(users.length)],
                            urls[random.nextInt(urls.length)],
                            new Date().getTime()
                    ));
                    Thread.sleep(2000L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        source.print();

        env.execute();
    }
}
