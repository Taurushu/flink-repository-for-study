package top.taurushu.pojo;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Event {

    private String name;
    private String uri;
    private Long time;

    public Event() {
    }

    public Event(String value) {
        this(value.split(",")[0], value.split(",")[1], Long.valueOf(value.split(",")[2]));
    }

    public Event(String name, String uri, Long time) {
        this.name = name;
        this.uri = uri;
        this.time = time;
    }
    
    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", uri='" + uri + '\'' +
                ", time=" + new Timestamp(time) +
                '}';
    }

}