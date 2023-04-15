package top.taurushu.init;

import lombok.Data;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

@Data
public class MysqlSourceTopic {
    private String catalog;
    private String database;
    private String table;

    public MysqlSourceTopic() {
    }

    public MysqlSourceTopic(String topic) {
        String[] infos = topic.split("\\.");
        this.catalog = infos[0];
        this.database = infos[1];
        this.table = infos[2];
    }
}
