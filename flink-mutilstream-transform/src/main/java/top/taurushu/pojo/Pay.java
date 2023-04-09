package top.taurushu.pojo;

import lombok.Data;

@Data
public class Pay {
    private String orderId;
    private String from;
    private Long timestampLong;

    public Pay(){

    }

    public Pay(String orderId, String from, Long timestampLong) {
        this.orderId = orderId;
        this.from = from;
        this.timestampLong = timestampLong;
    }
}
