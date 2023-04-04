package top.taurushu.streamSource;

import org.apache.flink.annotation.Public;

import java.sql.Timestamp;

public class Event {

    private String name;
    private String uri;
    private Long time;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Event() {
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