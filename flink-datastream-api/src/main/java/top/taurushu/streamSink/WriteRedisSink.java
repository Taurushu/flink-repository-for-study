package top.taurushu.streamSink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import top.taurushu.streamSource.DiyParallelSourceFunc;
import top.taurushu.streamSource.Event;

import java.net.InetSocketAddress;
import java.util.HashSet;

public class WriteRedisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> source = env.addSource(new DiyParallelSourceFunc()).setParallelism(2);

        HashSet<InetSocketAddress> inetSocketAddressHashSet = new HashSet<>();
        inetSocketAddressHashSet.add(new InetSocketAddress("node1", 7000));
        inetSocketAddressHashSet.add(new InetSocketAddress("node2", 7000));
        inetSocketAddressHashSet.add(new InetSocketAddress("node3", 7000));

        source.addSink(new RedisSink<>(
                        new FlinkJedisClusterConfig.Builder().setNodes(inetSocketAddressHashSet).build(),
                        new RedisMapper<Event>() {
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME", 3);
                            }

                            @Override
                            public String getKeyFromData(Event event) {
                                return event.getName();
                            }

                            @Override
                            public String getValueFromData(Event event) {
                                return event.toString();
                            }
                        }
                )
        );

        env.execute();
    }
}


