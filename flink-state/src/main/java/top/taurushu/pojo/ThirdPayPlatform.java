package top.taurushu.pojo;

import lombok.Data;

@Data
public class ThirdPayPlatform {
    private String orderId;
    private String from;
    private Boolean success;
    private Long time;

    public ThirdPayPlatform() {
    }

    public ThirdPayPlatform(String orderId, String from, Boolean success, Long time) {
        this.orderId = orderId;
        this.from = from;
        this.success = success;
        this.time = time;
    }
}
