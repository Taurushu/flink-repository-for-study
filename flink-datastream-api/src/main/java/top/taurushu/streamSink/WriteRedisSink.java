package top.taurushu.streamSink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        DataStream<Tuple2<String, String>> redisTuple = source.map(
                (Event value) -> new Tuple2<>(value.getName(), value.toString())
        ).returns(new TypeHint<Tuple2<String, String>>() {
        });

        HashSet<InetSocketAddress> inetSocketAddressHashSet = new HashSet<>();
        inetSocketAddressHashSet.add(new InetSocketAddress("node1", 7000));
        inetSocketAddressHashSet.add(new InetSocketAddress("node2", 7000));
        inetSocketAddressHashSet.add(new InetSocketAddress("node3", 7000));

        FlinkJedisClusterConfig conf = new FlinkJedisClusterConfig.Builder()
                .setNodes(inetSocketAddressHashSet)
                .build();

        redisTuple.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

        env.execute();
    }

    static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}


