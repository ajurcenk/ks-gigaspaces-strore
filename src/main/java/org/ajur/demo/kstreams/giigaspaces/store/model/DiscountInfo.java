package org.ajur.demo.kstreams.giigaspaces.store.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiscountInfo {

    private String customerCardNo;
    private Double totalAmount;
    private Double earnedLoyaltyPoints;
    private Double totalLoyaltyPoints;

    public String getCustomerCardNo() {
        return customerCardNo;
    }

    public void setCustomerCardNo(String customerCardNo) {
        this.customerCardNo = customerCardNo;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Double getEarnedLoyaltyPoints() {
        return earnedLoyaltyPoints;
    }

    public void setEarnedLoyaltyPoints(Double earnedLoyaltyPoints) {
        this.earnedLoyaltyPoints = earnedLoyaltyPoints;
    }

    public Double getTotalLoyaltyPoints() {
        return totalLoyaltyPoints;
    }

    public void setTotalLoyaltyPoints(Double totalLoyaltyPoints) {
        this.totalLoyaltyPoints = totalLoyaltyPoints;
    }

    @Override
    public String toString() {
        return "DiscountInfo{" +
                "customerCardNo='" + customerCardNo + '\'' +
                ", totalAmount=" + totalAmount +
                ", earnedLoyaltyPoints=" + earnedLoyaltyPoints +
                ", totalLoyaltyPoints=" + totalLoyaltyPoints +
                '}';
    }
}
