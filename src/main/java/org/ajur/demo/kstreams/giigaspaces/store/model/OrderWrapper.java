package org.ajur.demo.kstreams.giigaspaces.store.model;

import java.util.Date;

public class OrderWrapper {


    private Order order;
    private Date startTime;
    private Date endTime;

    public long getLatencyMs() {
        return latencyMs;
    }

    private long latencyMs;

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
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
        return "OrderWrapper{" +
                "order=" + order +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", latencyMs=" + latencyMs +
                '}';
    }
}
