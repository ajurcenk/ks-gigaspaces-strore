package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.gks.DiscountGigaSpacePropertiesExtractor;
import org.ajur.demo.kstreams.giigaspaces.store.gks.GigaSpacesStateStoreStoreBuilder;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfoWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.model.Invoice;
import org.ajur.demo.kstreams.giigaspaces.store.model.InvoiceWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.ajur.demo.kstreams.giigaspaces.store.tranformers.DiscountLogicProcessor;
import org.ajur.demo.kstreams.giigaspaces.store.tranformers.DiscountLogicProcessorWithTranStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.StoreBuilder;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.core.transaction.manager.DistributedJiniTxManagerConfigurer;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DiscountCalcAppWithTranStore {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "discount-calc-tran-store-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.alex.ga:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
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

        final PlatformTransactionManager ptm = new DistributedJiniTxManagerConfigurer().transactionManager();
        final GigaSpace client = new GigaSpaceConfigurer(configurer).transactionManager(ptm).clustered(true).gigaSpace();

        // Stores
        final Map<String, String> changleLogCfg = new HashMap<>();
        final String discountStoreName = DiscountLogicProcessorWithTranStore.STORE_NAME;

        final StoreBuilder discountStoreBuilder = new GigaSpacesStateStoreStoreBuilder(discountStoreName,
                client, Serdes.String(),
                String.class,
                discountSerde,
                DiscountInfo.class,
                new DiscountGigaSpacePropertiesExtractor(),
                ptm)
                .withLoggingEnabled(changleLogCfg);

        // Add transactions support
        ((GigaSpacesStateStoreStoreBuilder) discountStoreBuilder).withSpaceTransactionsEnabled();

        builder.addStateStore(discountStoreBuilder);

        // Invoices stream
        final KStream<String, Invoice>  invoicesInputStream = builder.stream(AppConfigs.TOPIC_INVOICES,
                Consumed.with(Serdes.String(), invoiceSerdeSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST)
                        .withName("invoicesInputStream"));

        // Add time tracking
        final KStream<String, InvoiceWrapper> invoicesWrapper = invoicesInputStream.mapValues((readOnlyKey, value) -> {

            final InvoiceWrapper wrapper = new InvoiceWrapper();
            wrapper.setInvoice(value);
            wrapper.setStartTime(new Date());

            return wrapper;
        });

        // Calculate customer discount
        final KStream<String, DiscountInfoWrapper> discountsStream = invoicesWrapper
                .transform(() -> {

                    return new DiscountLogicProcessorWithTranStore();
                },  Named.as("discountsProcessorWithTranStore"), discountStoreName);


        // Calculate latency
        final KStream<String, DiscountInfoWrapper> discountsWithLatencyStream =
                discountsStream.mapValues((readOnlyKey, value) -> {

                    value.setEndTime(new Date());
                    return value;
                });


        discountsWithLatencyStream.foreach((key, value) -> {

            System.out.println(String.format("Thread name: %s, Customer number: %s Discount info: %s LatencyMS: %d",
                    Thread.currentThread().getName(),
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