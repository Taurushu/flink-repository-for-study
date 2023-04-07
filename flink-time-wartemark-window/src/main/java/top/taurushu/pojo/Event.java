package top.taurushu.pojo;

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

    public Event(String value) {
        this(value.split(",")[0], value.split(",")[1], Long.valueOf(value.split(",")[2]));
    }

    public Event(String name, String uri, Long time) {
        this.name = name;
        this.uri = uri;
        this.time = time;
    }


    public String toCsv() {
        return name + ',' + uri + "," + time;
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