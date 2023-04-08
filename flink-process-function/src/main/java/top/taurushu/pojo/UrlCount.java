package top.taurushu.pojo;


import lombok.Data;

@Data
public class UrlCount {
    private String url;
    private Long count;
    private Long windowStart;
    private Long windowEnd;

    public UrlCount(){

    }

    public UrlCount(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
