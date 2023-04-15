package top.taurushu.init;

import lombok.Data;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

@Data
public class MysqlSourceInfo {
    private Map<String, ?> sourcePartition;
    private Map<String, ?> sourceOffset;
    private MysqlSourceTopic topic;
    private Integer kafkaPartition;
    private Struct key;
    private Struct keySchema;
    private Struct value;
    private Struct valueSchema;
    private Long timestamp;
    private Headers headers;


    public MysqlSourceInfo() {
    }

    public MysqlSourceInfo(SourceRecord sourceRecord) {
        this.sourcePartition = sourceRecord.sourcePartition();
        this.sourceOffset = sourceRecord.sourceOffset();
        this.topic = new MysqlSourceTopic(sourceRecord.topic());
        this.kafkaPartition = sourceRecord.kafkaPartition();
        this.key = (Struct) sourceRecord.key();
        this.keySchema = (Struct) sourceRecord.keySchema();
        this.value = (Struct) sourceRecord.value();
        this.valueSchema = (Struct) sourceRecord.valueSchema();
        this.timestamp = sourceRecord.timestamp();
        this.headers = sourceRecord.headers();
    }
}
