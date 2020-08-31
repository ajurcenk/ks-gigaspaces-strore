package org.ajur.demo.kstreams.giigaspaces.store.serdes;

import org.ajur.demo.kstreams.giigaspaces.store.model.OrderWrapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrderWrapperSerde implements Serde<OrderWrapper> {

    private  Serde<OrderWrapper> serde;

    public OrderWrapperSerde() {

     this.serde = SerdesFactory.from(OrderWrapper.class, false);
    }

    @Override
    public Serializer<OrderWrapper> serializer() {
        return this.serde.serializer();
    }

    @Override
    public Deserializer<OrderWrapper> deserializer() {
        return this.serde.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }
}
