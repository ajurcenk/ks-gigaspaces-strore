package org.ajur.demo.kstreams.giigaspaces.store.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Invoice {

    private String invoiceNumber;
    private String customerNo;
    private Double totalAmount;
    private Integer numberOfItems;

    public String getInvoiceNumber() {
        return invoiceNumber;
    }

    public void setInvoiceNumber(String invoiceNumber) {
        this.invoiceNumber = invoiceNumber;
    }

    public String getCustomerNo() {
        return customerNo;
    }

    public void setCustomerNo(String customerNo) {
        this.customerNo = customerNo;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Integer getNumberOfItems() {
        return numberOfItems;
    }

    public void setNumberOfItems(Integer numberOfItems) {
        this.numberOfItems = numberOfItems;
    }

    @Override
    public String toString() {
        return "Invoice{" +
                "invoiceNumber='" + invoiceNumber + '\'' +
                ", customerNo='" + customerNo + '\'' +
                ", totalAmount=" + totalAmount +
                ", numberOfItems=" + numberOfItems +
                '}';
    }
}
