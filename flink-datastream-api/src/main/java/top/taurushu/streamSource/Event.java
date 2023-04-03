package top.taurushu.streamSource;

import java.sql.Timestamp;

public class Event {
    public String name;
    public String uri;
    public Long time;

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
