package org.ajur.demo.kstreams.giigaspaces.store.model;

import java.util.Date;

public class DiscountInfoWrapper {


    private DiscountInfo discountInfo;
    private Date startTime;
    private Date endTime;

    public long getLatencyMs() {

        return latencyMs;
    }

    private long latencyMs;

    public DiscountInfo getDiscountInfo() {
        return discountInfo;
    }

    public void setDiscountInfo(DiscountInfo discountInfo) {

        this.discountInfo = discountInfo;
    }

    public Date getStartTime() {

        return startTime;
    }

    public void setStartTime(Date startTime) {

        this.startTime = startTime;
    }

    public Date getEndTime() {

        return endTime;
    }

    public void setEndTime(Date endTime) {

        if (endTime != null) {
            this.endTime = endTime;
            this.latencyMs = endTime.getTime() - this.startTime.getTime();
        }
    }


    @Override
    public String toString() {
        return "DiscountInfoWrapper{" +
                "discountInfo=" + discountInfo +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", latencyMs=" + latencyMs +
                '}';
    }
}
