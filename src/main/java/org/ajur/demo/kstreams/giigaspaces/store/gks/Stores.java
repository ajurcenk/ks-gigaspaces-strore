package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.openspaces.core.GigaSpace;

import java.util.Objects;

public final class Stores {

    /**
     * Basic
     *
     * @param name
     * @param client
     * @return
     */
    public static KeyValueBytesStoreSupplier inMemoryKeyValueStore(final String name, GigaSpace client) {

        Objects.requireNonNull(name, "name cannot be null");

        return new KeyValueBytesStoreSupplier() {

            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {

                return new GigaSpacesKeyValueStoreOld(client, name);

            }

            @Override
            public String metricsScope() {

                return "in-memory";
            }
        };
    }

    /**
     * With value and key types
     *
     * @param name
     * @param client
     * @param keySerde
     * @param keyType
     * @param valueSerde
     * @param valueType
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K,V> KeyValueBytesStoreSupplier inMemoryKeyValueStore(final String name, GigaSpace client,
                                                                       Serde<K> keySerde, Class<K> keyType,
                                                                       Serde<V> valueSerde,Class<V> valueType
    ) {

        return new KeyValueBytesStoreSupplier() {

            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {

                return new GigaSpacesKeyValueStoreOld(client, name, keyType, valueType, keySerde, valueSerde);

            }

            @Override
            public String metricsScope() {

                return "in-memory";
            }
        };
    }


    /**
     * With space property extractor
     *
     * @param name
     * @param client
     * @param keySerde
     * @param keyType
     * @param valueSerde
     * @param valueType
     * @param spacePropertiesExtractor
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K,V> KeyValueBytesStoreSupplier inMemoryKeyValueStore(final String name, GigaSpace client,
                                                                         Serde<K> keySerde, Class<K> keyType,
                                                                         Serde<V> valueSerde,Class<V> valueType,
                                                                         GigaSpacePropertiesExtractor<V> spacePropertiesExtractor
    ) {

        return new KeyValueBytesStoreSupplier() {

            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {

                return new GigaSpacesKeyValueStoreOld(client, name, keyType, valueType, keySerde, valueSerde,spacePropertiesExtractor);

            }

            @Override
            public String metricsScope() {

                return "in-memory";
            }
        };
    }
}
