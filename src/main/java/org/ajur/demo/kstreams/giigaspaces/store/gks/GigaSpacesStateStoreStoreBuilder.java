package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.GigaSpacesChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.GigaSpacesMeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.openspaces.core.GigaSpace;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GigaSpacesStateStoreStoreBuilder<K,V extends StateStore> implements StoreBuilder<KeyValueStore<Bytes, byte[]>> {

    private String storeName;
    private GigaSpace client;
    private Serde<K> keySerde;
    private Class<K> keyType;
    private Serde<V> valueSerde;
    private Class<V> valueType;

    private Map<String, String> logConfig = new HashMap<>();
    boolean enableCaching;
    boolean enableLogging = false;

    public GigaSpacesStateStoreStoreBuilder(String storeName, GigaSpace client, Serde<K> keySerde,
                                            Class<K> keyType, Serde<V> valueSerde, Class<V> valueType,
                                            GigaSpacePropertiesExtractor<V> spacePropertiesExtractor) {
        this.storeName = storeName;
        this.client = client;
        this.keySerde = keySerde;
        this.keyType = keyType;
        this.valueSerde = valueSerde;
        this.valueType = valueType;
        this.spacePropertiesExtractor = spacePropertiesExtractor;
    }

    private GigaSpacePropertiesExtractor<V> spacePropertiesExtractor;

    @Override
    public StoreBuilder< KeyValueStore<Bytes, byte[]>> withCachingEnabled() {
        return this;
    }

    @Override
    public StoreBuilder< KeyValueStore<Bytes, byte[]>> withCachingDisabled() {
        return this;
    }



    @Override
    public StoreBuilder< KeyValueStore<Bytes, byte[]>> withLoggingEnabled(final Map<String, String> config) {
        Objects.requireNonNull(config, "config can't be null");
        enableLogging = true;
        logConfig = config;
        return this;
    }

    @Override
    public StoreBuilder< KeyValueStore<Bytes, byte[]>> withLoggingDisabled() {

        enableLogging = false;
        logConfig.clear();
        return this;
    }

    @Override
    public Map<String, String> logConfig() {

        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {

        return enableLogging;
    }


    @Override
    public String name() {
        return storeName;
    }

    @Override
    public  KeyValueStore<Bytes, byte[]> build() {

       final  GigaSpacesStateStore store =  new GigaSpacesStateStore(client, storeName,
                keyType, valueType, keySerde,
                valueSerde,spacePropertiesExtractor);

        final KeyValueStore<Bytes, byte[]> loggingStore =   maybeWrapLogging(store);

        final KeyValueStore<Bytes, byte[]> meteredStore =  new GigaSpacesMeteredKeyValueStore(
                loggingStore,
                "giga-store-in-memory",
                Time.SYSTEM,
                keySerde,
                valueSerde);

        return meteredStore;

    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(final KeyValueStore<Bytes, byte[]> inner) {


        if (!enableLogging) {

            return inner;
        }

        return new GigaSpacesChangeLoggingKeyValueBytesStore(inner);
    }
}
