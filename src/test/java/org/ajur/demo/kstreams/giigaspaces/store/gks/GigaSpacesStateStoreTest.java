package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;
import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.GigaSpacesChangeLoggingKeyValueBytesStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class GigaSpacesStateStoreTest {

    protected InternalMockProcessorContext context;
    protected ValueAndTimestampSerde<Customer> valueAndTimestampDes;

    protected GigaSpace client;
    protected UrlSpaceConfigurer configurer;
    protected KeyValueStoreTestDriver<String, String> driver;
    protected KeyValueStore<String, String> store;

    @Before
    public void setUp() {


        // TODO Create space automatically

        // GigaSpaces Client
        configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        client = new GigaSpaceConfigurer(configurer).clustered(true).gigaSpace();

        this.driver = KeyValueStoreTestDriver.create(String.class, String.class);
        this.context = (InternalMockProcessorContext) this.driver.context();
        this.context.setTime(10L);
        this.store = this.createGigaSpacesKeyValueStore(this.context);


        this.context.setValueSerde(Serdes.String());
        this.context.setKeySerde(Serdes.String());

    }

    @After
    public void clean() {

        this.store.close();
        this.driver.clear();
        configurer.close();
    }

    protected <K, V> KeyValueStore<K, V> createKeyValueInMemoryStoreStore(ProcessorContext context) {

        StoreBuilder storeBuilder = org.apache.kafka.streams.state.Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), context.keySerde(), context.valueSerde()).withLoggingEnabled(Collections.singletonMap("retention.ms", "1000"));

        StateStore store = storeBuilder.build();
        store.init(context, store);

        return (KeyValueStore)store;
    }

    protected KeyValueStore<String, String> createGigaSpacesKeyValueStore(ProcessorContext context) {


        final String storeName = "my-test-gigaspace-state-store";

        final Map<String, String> changleLogCfg = new HashMap<>();

        // Create store with logging
        final StoreBuilder storeBuilder = new GigaSpacesStateStoreStoreBuilder(storeName,
                client, this.context.keySerde(),
               String.class, this.context.valueSerde(), String.class, null);

        // Enable logging
        storeBuilder.withLoggingEnabled(changleLogCfg);

        final StateStore store = storeBuilder.build();
        store.init(context, store);

        return  (KeyValueStore) store;
    }


    @Test
    public void putAndGet() {


        final KeyValueStore<String, String> store = this.store;
        final String key = "key-4";
        final String value = "test-4";

        // put
        store.put(key, value);

        // get
        final String newValue = store.get(key);

        assertTrue(newValue.equals(value));

    }


    @Test
    public void delete() {

        final KeyValueStore<String, String> store = this.store;
        final String key = "key-7";
        final String value = "test-4";

        store.put(key, value);

        store.delete(key);

        assertTrue(store.get(key) == null);

    }

    @Test
    public void putUpdateAndGet() {


        final KeyValueStore<String, String> store = this.store;
        final String key = "key-7";
        final String value = "test-7";
        final String newValue = "test-7-update";

        // insert
        store.put(key, value);

        // update
        store.put(key, newValue);

        // get
        final String newValueAfterGet = store.get(key);

        assertTrue(newValueAfterGet.equals(newValue));

    }

    @Test
    public void getAll() {


        final KeyValueStore<String, String> store = this.store;

        for (int i = 0; i < 3; i++) {

            final String key = String.format("key-%d", i);
            final String value = String.format("test-%d", i);
            store.put(key, value);
        }


        final KeyValueIterator<String, String> iterator = store.all();

        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();
            count++;
        }

        assertTrue(count >0 );
    }


    @Test
    public void range() {


        final KeyValueStore<String, String> store = this.store;

        for (int i = 0; i <= 3; i++) {

            final String key = String.format("key-%d", i);
            final String value = String.format("test-%d", i);
            store.put(key, value);
        }


        final String fromKey = "key-1";
        final String toKey = "key-3";
        final KeyValueIterator<String, String> iterator = store.range(fromKey, toKey);

        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();

            count++;
        }

       assertTrue(count == 2);
    }


    @Test
    public void putAll() {

        final KeyValueStore<String, String> store = this.store;

        final List<KeyValue<String, String>> data = new ArrayList<>();

        for (int i = 100; i <= 103; i++) {

            final String key = String.format("key-%d", i);
            final String value = String.format("test-%d", i);

            data.add(KeyValue.pair(key, value));
        }

        store.putAll(data);

        data.forEach(keyVal -> {

            assertTrue(keyVal.value.equals(store.get(keyVal.key)));
        });

    }

    @Test
    public void putIfAbsent() {

        final KeyValueStore<String, String> store = this.store;

        final String key = "key-200";
        final String value = "test-200";
        final String newValue = "test-200-update";

        store.put(key, value);

        store.putIfAbsent(key, newValue);

        assertTrue(value.equals(store.get(key)));

        store.delete(key);

        store.putIfAbsent(key, newValue);

        assertTrue(newValue.equals(store.get(key)));
    }

    @Test
    public void querySingle() {

        final KeyValueStore<String, String> store = this.store;
        final String key = "key-4";
        final String value = "test-4";


        // TODO Move to utility class
        final StateStore stateStore =  ((WrappedStateStore)((WrappedStateStore) store).wrapped()).wrapped();

        assertTrue(GigaSpacesStateStore.class.getName().equals(stateStore.getClass().getName()));

        // put
        store.put(key, value);

        final GigaSpacesStateStore<String,String> gigaStore = (GigaSpacesStateStore) stateStore;

        final Object[] params = {key};

        // query
        final SpaceDocument spaceDocument = gigaStore.querySingle("strongTypedKey = ?",params);

        final String valueFromQuery = spaceDocument.getProperty(GigaSpacesStateStore.STRONG_TYPED_VALUE);

        assertTrue(value.equals(valueFromQuery));

    }


}
