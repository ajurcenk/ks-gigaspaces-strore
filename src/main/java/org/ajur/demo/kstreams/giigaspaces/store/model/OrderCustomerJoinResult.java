package org.ajur.demo.kstreams.giigaspaces.store.model;

public class OrderCustomerJoinResult {

    private OrderWrapper order;
    private Customer customer;

    public OrderWrapper getOrder() {
        return order;
    }

    public void setOrder(OrderWrapper order) {
        this.order = order;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    @Override
    public String toString() {
        return "OrderCusomerJoinResult{" +
                "order=" + order +
                ", customer=" + customer +
                '}';
    }
}
