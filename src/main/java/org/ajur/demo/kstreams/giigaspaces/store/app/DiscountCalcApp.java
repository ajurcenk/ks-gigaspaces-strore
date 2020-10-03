package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.gks.DiscountGigaSpacePropertiesExtractor;
import org.ajur.demo.kstreams.giigaspaces.store.gks.GigaSpacesStateStoreStoreBuilder;
import org.ajur.demo.kstreams.giigaspaces.store.model.*;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.ajur.demo.kstreams.giigaspaces.store.tranformers.DiscountLogicProcessor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.StoreBuilder;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DiscountCalcApp {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "discount-calc-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.alex.ga:9092");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final Serde<Invoice> invoiceSerdeSerde = SerdesFactory.from(Invoice.class);
        final Serde<DiscountInfo> discountSerde = SerdesFactory.from(DiscountInfo.class);

        final StreamsBuilder builder = new StreamsBuilder();

        final UrlSpaceConfigurer configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner");
        final GigaSpace client = new GigaSpaceConfigurer(configurer).clustered(true).gigaSpace();

        // Stores
        final Map<String, String> changleLogCfg = new HashMap<>();
        final String discountStoreName = "discount-store";
        final StoreBuilder discountStoreBuilder = new GigaSpacesStateStoreStoreBuilder(discountStoreName,
                client, Serdes.String(),
                String.class, discountSerde, DiscountInfo.class, new DiscountGigaSpacePropertiesExtractor())
                .withLoggingEnabled(changleLogCfg);
        builder.addStateStore(discountStoreBuilder);

        // Invoices stream
        final KStream<String, Invoice>  invoicesInputStream = builder.stream(AppConfigs.TOPIC_INVOICES,
                Consumed.with(Serdes.String(), invoiceSerdeSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST)
                        .withName("invoicesInputStream"));

        // Add time tracking
        final KStream<String, InvoiceWrapper> invovesWrapper = invoicesInputStream.mapValues((readOnlyKey, value) -> {

            final InvoiceWrapper wrapper = new InvoiceWrapper();
            wrapper.setInvoice(value);
            wrapper.setStartTime(new Date());

            return wrapper;
        });

        // Join customers and orders
        final KStream<String, DiscountInfoWrapper> discountsStream = invovesWrapper
                .transform(() -> {

                    return new DiscountLogicProcessor();
                },  Named.as("discountsProcessor"), discountStoreName);


        // Calculate latency
        final KStream<String, DiscountInfoWrapper> discountsWithLatencyStream =
                discountsStream.mapValues((readOnlyKey, value) -> {

                    value.setEndTime(new Date());
                    return value;
                });


        discountsWithLatencyStream.foreach((key, value) -> {

            System.out.println(String.format("Customer number: %s Discount info: %s LatencyMS: %d",
                    value.getDiscountInfo().getCustomerCardNo(), value.getDiscountInfo().toString(),
                    value.getLatencyMs()));
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