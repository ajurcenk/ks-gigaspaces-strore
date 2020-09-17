package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.Invoice;
import org.ajur.demo.kstreams.giigaspaces.store.model.Order;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.invoices  --partitions 3 --replication-factor 1 --command-config ./cfg/client.properties
 * kafka-console-consumer --bootstrap-server  kafka.alex.ga:9092 --topic testks.invoices --from-beginning --consumer.config  ./cfg/client.properties
 */

public class InvoicesSimulator {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        simulateInvoices();
        //simulateCustomers();
    }


    public static void simulateInvoices() throws ExecutionException, InterruptedException {


        final int numberOfCustomers= 1;
        int customerIdIdStart = 100;

        int i = 0;

        final int[] customerIds = new int[numberOfCustomers + 1];

        while(i <= numberOfCustomers) {

            customerIds[i] = i;

            i++;
            customerIdIdStart++;
        }

        i = 0;
        final int numberOfOrders = 10;
        int ordersIdStart = 5000;

        final Producer<String, Invoice> orderProducer = createInvoiceProducer();

        while(i <= numberOfOrders) {

            final String customerId = "110";
                    //"108";
                    //"" + getRandom(customerIds);

            final Invoice invoice = new Invoice();
            invoice.setInvoiceNumber("" + ordersIdStart);
            invoice.setCustomerNo(customerId);

            int leftLimitNumberOfItems = 1;
            int rightLimitNumberOfItems = 50;
            final int numberOfItems = leftLimitNumberOfItems +
                    (int) (new Random().nextFloat() * (rightLimitNumberOfItems - leftLimitNumberOfItems));
            invoice.setNumberOfItems(numberOfItems);

            double leftLimitTotalAmount = 1D;
            double rightLimiTotalAmountt = 100D;
            final double amount = leftLimitTotalAmount +
                    new Random().nextDouble() * (rightLimiTotalAmountt - leftLimitTotalAmount);
            invoice.setTotalAmount(amount);


            final ProducerRecord<String, Invoice> record = new ProducerRecord<>(AppConfigs.TOPIC_INVOICES,
                    invoice.getInvoiceNumber(), invoice);

            System.out.println("Sending invoice: " + invoice);
            orderProducer.send(record).get();

            Thread.sleep(1000L);

            i++;
            ordersIdStart++;
        }

    }



    public static Producer<String, Invoice> createInvoiceProducer() {

        final Producer<String, Invoice> producer = new KafkaProducer<>(AppConfigs.getProducerConfig(),
                new StringSerializer(),
                SerdesFactory.from(Invoice.class, false).serializer());

        return producer;
    }



    private static int getRandom(int[] array) {

        int rnd = (int)(Math.random()*array.length);

        return array[rnd];
    }


}



