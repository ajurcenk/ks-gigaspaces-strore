package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.gks.CustomerGigaSpacePropertiesExtractor;
import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.Order;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderCustomerJoinResult;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.ajur.demo.kstreams.giigaspaces.store.tranformers.OrdersJoinerTransformer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.Date;
import java.util.Properties;

/**
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic testks.orders  --command-config ./cfg/client.properties
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic testks.customers   --command-config ./cfg/client.properties
 *
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --delete --topic orders-joiner-v1-customers-changelog  --command-config ./cfg/client.properties
 *
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.customers  --partitions 9 --replication-factor 3 --command-config ./cfg/client.properties
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.orders     --partitions 9 --replication-factor 3 --command-config ./cfg/client.properties
 *
 * kafka-console-producer --bootstrap-server kafka.alex.ga:9092 --topic  testks.customers --producer.config ./cfg/client.properties  --property "parse.key=true" --property "key.separator=:"
 *
 *
 * space query: select customer_name, count(customer_name)  from customers where rowNum<100  group by customer_name
 */

public class OrdersJoinerApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-joiner-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.alex.ga:9092");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

       final String storeName = "customers";
       final String inputTopicCustomers = "testks.customers";


       final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);
       final Serde<Order> orderSerde = SerdesFactory.from(Order.class);


       StreamsBuilder builder = new StreamsBuilder();

        final UrlSpaceConfigurer configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        final GigaSpace client = new GigaSpaceConfigurer(configurer).clustered(true).gigaSpace();

        // Customers dimension table
        KTable<String,Customer> tableCustomer = builder.table(inputTopicCustomers,
                Materialized.<String,Customer>as( org.ajur.demo.kstreams.giigaspaces.store.gks.Stores.inMemoryKeyValueStore(storeName, client,
                        Serdes.String(), String.class,
                        customerSerde, Customer.class, new CustomerGigaSpacePropertiesExtractor()))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde)
                        .withCachingDisabled());

        tableCustomer.toStream().print(Printed.<String,Customer>toSysOut().withLabel("customer:"));


        // Orders stream
        final KStream<String, Order> orderLineChangeEvtStream = builder.stream(AppConfigs.TOPIC_ORDERS,
                Consumed.with(Serdes.String(), orderSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST)
                        .withName("ordersInputStream"));

        // Add time tracking
        KStream<String, OrderWrapper> orderWrappers = orderLineChangeEvtStream.mapValues((readOnlyKey, value) -> {

                final OrderWrapper wrapper = new OrderWrapper();
                wrapper.setOrder(value);
                wrapper.setStartTime(new Date());

                return wrapper;
        });

        // Join customers and orders
        final KStream<String, OrderCustomerJoinResult> ordersAndCustomerJoin = orderWrappers
                .transform(() -> {

                    return new OrdersJoinerTransformer(client, customerSerde);
                });


        // Calculate latency
        final KStream<String, OrderCustomerJoinResult> ordersAndCustomerJoinWithLatency =
                ordersAndCustomerJoin.mapValues((readOnlyKey, value) -> {

            value.getOrder().setEndTime(new Date());
            return value;
        });


        ordersAndCustomerJoinWithLatency.foreach((key, value) -> {

            System.out.println(String.format("Order Id: %s Customer Id: %s LatencyMS: %d",
                    value.getOrder().getOrder().getId(), value.getCustomer().getId(),
                    value.getOrder().getLatencyMs()));

        });

        Topology topology =  builder.build();

        System.out.println(topology.describe());

        KafkaStreams stream = new KafkaStreams(topology, props);
        // Clean local state
        stream.cleanUp();

        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stream.close();
            configurer.close();
        }));


    }
}
