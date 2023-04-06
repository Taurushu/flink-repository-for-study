package top.taurushu.streamSink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import top.taurushu.streamSource.DiyParallelSourceFunc;
import top.taurushu.streamSource.Event;

import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;

public class WriteMysqlSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.addSource(new DiyParallelSourceFunc()).setParallelism(2);


        SinkFunction<Event> sink = JdbcSink.sink(
                "insert into events (name, uri, time) values (?, ?, ?)",                       // mandatory
                (PreparedStatement preparedStatement, Event event) -> {
                    preparedStatement.setString(1, event.getName());
                    preparedStatement.setString(2, event.getName());
                    preparedStatement.setLong(3, event.getTime());
                },// mandatory
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(200)             // optional: default = 0, meaning no time-based execution is done
                        .withBatchSize(1000)                  // optional: default = 5000 values
                        .withMaxRetries(5)                    // optional: default = 3
                        .build(),                  // optional
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://node1:3306/flinkSink?useSSL=false")
                        .withUsername("root")
                        .withPassword("shujie")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .build()                  // mandatory
        );

        source.addSink(sink);

        env.execute();
    }
}


