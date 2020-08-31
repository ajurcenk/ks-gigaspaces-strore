package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class AppConfigs {


    public static String BOOTSTRAP_SERVERS = "kafka.alex.ga:9092";

    public static String TOPIC_ORDERS = "testks.orders";
    public static String TOPIC_CUSTOMERS = "testks.customers";


    public static Properties getSecurityConfig() {

        final Properties props = new Properties();

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");

        return props;
    }

    public static Properties getProducerConfig() {

        final Properties props = new Properties();

        props.putAll(getSecurityConfig());

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "1");


        return props;
    }

}
