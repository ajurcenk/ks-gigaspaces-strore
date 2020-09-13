package org.ajur.demo.kstreams.giigaspaces.store.model;

import java.util.Date;

public class InvoiceWrapper {


    private Invoice invoice;
    private Date startTime;
    private Date endTime;

    public long getLatencyMs() {
        return latencyMs;
    }

    private long latencyMs;

    public Invoice getInvoice() {
        return invoice;
    }

    public void setInvoice(Invoice invoice) {

        this.invoice = invoice;
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
        return "InvoiceWrapper{" +
                "invoice=" + invoice +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", latencyMs=" + latencyMs +
                '}';
    }
}
