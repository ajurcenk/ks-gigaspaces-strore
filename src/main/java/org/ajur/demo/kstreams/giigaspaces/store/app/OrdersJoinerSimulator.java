package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.Order;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.ExecutionException;

/**
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.orders  --partitions 1 --replication-factor 1 --command-config ./cfg/client.properties
 *
 */

public class OrdersJoinerSimulator {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        simulateOrders();
        //simulateCustomers();
    }




    public static void simulateCustomers() throws ExecutionException, InterruptedException {


        final int numberOfCustomers = 1000;
        int customersIdStart = 1;

        final Producer<String, Customer> customerProducer = createCustomerProducer();

        int i = 0;

        while(i <= numberOfCustomers) {


            final Customer customer = new Customer();
            customer.setId("" + customersIdStart);
            customer.setName("" + customersIdStart);

            final ProducerRecord<String, Customer> record = new ProducerRecord<>(AppConfigs.TOPIC_CUSTOMERS,
                    customer.getId(), customer);

            System.out.println("Sending customer: " + customer);
            customerProducer.send(record).get();

            i++;
            customersIdStart++;
        }

    }

    public static void simulateOrders() throws ExecutionException, InterruptedException {


        final int numberOfCustomers = 1000;
        int customersIdStart = 1;
        int i = 0;

        final int[] customerIds = new int[numberOfCustomers + 1];

        while(i <= numberOfCustomers) {

            customerIds[i] = i;

            i++;
            customersIdStart++;
        }

        i = 0;
        final int numberOfOrders = 10;
        int ordersIdStart = 5000;

        final Producer<String, Order> orderProducer = createOrderProducer();

        while(i <= numberOfOrders) {

            final String customerId = "" + getRandom(customerIds);

            final Order order = new Order();
            order.setId("" + ordersIdStart);
            order.setCustomerId(customerId);
            order.setDescription(String.format("Order id: %s for customer: %s", order.getId(), order.getCustomerId()));

            final ProducerRecord<String, Order> record = new ProducerRecord<>(AppConfigs.TOPIC_ORDERS, order.getId(), order);

            System.out.println("Sending order: " + order);
            orderProducer.send(record).get();

            Thread.sleep(1000L);

            i++;
            ordersIdStart++;
        }

    }

    public static Producer<String, Order> createOrderProducer() {

        final Producer<String, Order> producer = new KafkaProducer<String, Order>(AppConfigs.getProducerConfig(),
                new StringSerializer(),
                SerdesFactory.from(Order.class, false).serializer());

        return producer;
    }


    public static Producer<String, Customer> createCustomerProducer() {

        final Producer<String, Customer> producer = new KafkaProducer<>(AppConfigs.getProducerConfig(),
                new StringSerializer(),
                SerdesFactory.from(Customer.class, false).serializer());

        return producer;
    }



    private static int getRandom(int[] array) {

        int rnd = (int)(Math.random()*array.length);

        return array[rnd];
    }


}



