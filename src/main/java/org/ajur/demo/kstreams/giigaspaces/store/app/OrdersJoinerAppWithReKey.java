package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.Order;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderCustomerJoinResult;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.AbstractStream;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;

import java.util.Date;
import java.util.Properties;

/**
 * kafka-topics  --bootstrap-server  kafka.alex.ga:9092 --create --topic testks.customers  --partitions 1 --replication-factor 1 --command-config ./cfg/client.properties
 *
 * kafka-console-producer --bootstrap-server kafka.alex.ga:9092 --topic  testks.customers --producer.config ./cfg/client.properties  --property "parse.key=true" --property "key.separator=:"
 *
 *
 *  kafka-streams-application-reset --application-id=orders-joiner-withrekey-v1 --bootstrap-servers kafka.alex.ga:9092 --input-topics testks.customers --to-earliest --config-file ./cfg/client.properties
 */
public class OrdersJoinerAppWithReKey {

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-joiner-withrekey-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.alex.ga:9092");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG,  "/tmp/kstreams/state");


       final String storeName = "customers";
       final String inputTopicCustomers = AppConfigs.TOPIC_CUSTOMERS;

       final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);
       final Serde<Order> orderSerde = SerdesFactory.from(Order.class);
       final Serde<OrderWrapper> orderWrapperSerde = SerdesFactory.from(OrderWrapper.class, false);


       StreamsBuilder builder = new StreamsBuilder();


        // Customers dimension table
        KTable<String,Customer> tableCustomer = builder.table(inputTopicCustomers,
                Consumed.with(Serdes.String(), customerSerde));

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

       // Re-key orders by customer id
      KStream<String, OrderWrapper> rekeyByCustId = orderWrappers.selectKey((key, value) -> value.getOrder().getCustomerId());


      final AbstractStream ksImp = (KStreamImpl) rekeyByCustId;
      org.apache.kafka.streams.kstream.internals.Utils.changeSerde(ksImp, orderWrapperSerde);

        // Join with customers
        KStream<String, OrderCustomerJoinResult> orderWithCustomer = rekeyByCustId.join(tableCustomer, (orderWrapper, customer) -> {

            final OrderCustomerJoinResult joinResult = new OrderCustomerJoinResult();
            joinResult.setCustomer(customer);
            joinResult.setOrder(orderWrapper);

            return joinResult;
        });

        // Calculate latency
        final KStream<String, OrderCustomerJoinResult> ordersAndCustomerJoinWithLatency =
                orderWithCustomer.mapValues((readOnlyKey, value) -> {

                    value.getOrder().setEndTime(new Date());
                    return value;
                });

        // Print result
        ordersAndCustomerJoinWithLatency.foreach((key, value) -> {

            System.out.println(String.format("Order Id: %s Customer Id: %s LatencyMS: %d",
                    value.getOrder().getOrder().getId(), value.getCustomer().getId(),
                    value.getOrder().getLatencyMs()));

        });

        Topology topology =  builder.build();
        System.out.println(topology.describe());

        KafkaStreams stream = new KafkaStreams(topology, props);
        // Clean local state
        //stream.cleanUp();

        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stream.close();
        }));


    }
}
