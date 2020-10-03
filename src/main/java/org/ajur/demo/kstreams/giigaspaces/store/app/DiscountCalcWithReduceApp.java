package org.ajur.demo.kstreams.giigaspaces.store.app;

import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfoWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.model.Invoice;
import org.ajur.demo.kstreams.giigaspaces.store.model.InvoiceWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Date;
import java.util.Properties;

public class DiscountCalcWithReduceApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "discount-calc-with-reduce-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.alex.ga:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_PLAINTEXT");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final Serde<Invoice> invoiceSerdeSerde = SerdesFactory.from(Invoice.class);
        final Serde<DiscountInfo> discountSerde = SerdesFactory.from(DiscountInfo.class);
        final Serde<DiscountInfoWrapper> discountInfoWrapperSerde = SerdesFactory.from(DiscountInfoWrapper.class);

        final StreamsBuilder builder = new StreamsBuilder();

        // Invoices stream
        final KStream<String, Invoice> invoicesInputStream = builder.stream(AppConfigs.TOPIC_INVOICES,
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

        // Create discount and change the key to the customer number
        final KStream<String, DiscountInfoWrapper> customerDiscounts = invoicesWrapper.map( (key, invoiceWrapper)-> {

            final DiscountInfo discountInfo = new DiscountInfo();
            discountInfo.setCustomerCardNo(invoiceWrapper.getInvoice().getCustomerNo());
            discountInfo.setTotalAmount(invoiceWrapper.getInvoice().getTotalAmount());
            discountInfo.setEarnedLoyaltyPoints(1d);
            discountInfo.setTotalLoyaltyPoints(1d);

            final DiscountInfoWrapper discountInfoWrapper = new DiscountInfoWrapper();
            discountInfoWrapper.setStartTime(invoiceWrapper.getStartTime());
            discountInfoWrapper.setDiscountInfo(discountInfo);

            final KeyValue<String, DiscountInfoWrapper> keyVal = KeyValue.pair(invoiceWrapper.getInvoice().getCustomerNo(),
                    discountInfoWrapper);

            return keyVal;
        });

        // Group discounts by customer number
        final KGroupedStream<String, DiscountInfoWrapper> discountsByCustomerNo =
                customerDiscounts.groupByKey(Grouped.with(Serdes.String(), discountInfoWrapperSerde));


        // Calculate discount
        final KTable<String, DiscountInfoWrapper> customerDiscountsTbl = discountsByCustomerNo.reduce((aggValue, newValue) -> {

            newValue.getDiscountInfo()
                    .setTotalAmount(newValue.getDiscountInfo().getTotalAmount() + aggValue.getDiscountInfo().getTotalAmount());

            newValue.getDiscountInfo()
                    .setTotalLoyaltyPoints(newValue.getDiscountInfo().getTotalLoyaltyPoints() == null? 0.0d : newValue.getDiscountInfo().getTotalLoyaltyPoints()
                            + aggValue.getDiscountInfo().getTotalLoyaltyPoints());


            return newValue;
        });

        // Calculate latency
        final KStream<String, DiscountInfoWrapper> discountsWithLatencyStream =
                customerDiscountsTbl.toStream().mapValues((readOnlyKey, value) -> {

                    value.setEndTime(new Date());
                    return value;
                });

        // Print results
        discountsWithLatencyStream.foreach((key, discountWrapper) -> {

            System.out.println(String.format("Thread name: %s, Customer number: %s Discount info: %s LatencyMS: %d",
                    Thread.currentThread().getName(),
                    discountWrapper.getDiscountInfo().getCustomerCardNo(), discountWrapper.getDiscountInfo().toString(),
                    discountWrapper.getLatencyMs()));

        });


        Topology topology =  builder.build();

        System.out.println(topology.describe());

        KafkaStreams stream = new KafkaStreams(topology, props);
        // Clean local state
        stream.cleanUp();

        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stream.close();
        }));

    }

}
