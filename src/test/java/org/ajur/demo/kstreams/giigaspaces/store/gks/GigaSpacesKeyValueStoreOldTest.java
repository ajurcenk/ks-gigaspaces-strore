package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GigaSpacesKeyValueStoreOldTest {

    protected InternalMockProcessorContext context;
    protected ValueAndTimestampSerde<Customer> valueAndTimestampDes;
    protected GigaSpace client;
    UrlSpaceConfigurer configurer;


    @After
    public void clean() {

       configurer.close();
    }

    @Before
    public void setUp() {

        // TODO Create space automatically
        // Client
        configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        client = new GigaSpaceConfigurer(configurer).clustered(true).gigaSpace();

        ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1000, new MockStreamsMetrics(new Metrics()));
        context = new InternalMockProcessorContext(null, null, null, null, cache);

        final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);
        valueAndTimestampDes =  new ValueAndTimestampSerde<Customer>(customerSerde);
    }

    protected GigaSpacesKeyValueStoreOld<String, Customer> createKeyValueStore() {


        final String storeName = "my-test-store";
        final Serde<Customer> customerSerde = SerdesFactory.from(Customer.class);

        GigaSpacesKeyValueAbstractStoreBuilder storeBuilder = new GigaSpacesKeyValueAbstractStoreBuilder(storeName,
                client, Serdes.String(),
               String.class, customerSerde, Customer.class,
                new CustomerGigaSpacePropertiesExtractor());


        final StateStore store = storeBuilder.build();
        store.init(context, store);

        return  (GigaSpacesKeyValueStoreOld<String, Customer>) store;
    }


    @Test
    public void getAll() {


        final GigaSpacesKeyValueStoreOld<String, Customer> store = createKeyValueStore();
        final Customer cust1 = new Customer();
        cust1.setId("test-46");
        cust1.setName("test-46");

        store.put(this.bytesKey("test-43"), bytesValue(cust1));

        final KeyValueIterator<Bytes, byte[]> iterator = store.all();
        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();
            count++;
        }

        assertTrue(count >0 );

        System.out.println(count);

    }

    @Test
    public void range() {


        final GigaSpacesKeyValueStoreOld<String, Customer> store = createKeyValueStore();

        final Customer cust1 = new Customer();
        cust1.setId("1");
        cust1.setName("cust_1");
        store.put(this.bytesKey(cust1.getId()), bytesValue(cust1));


        final Customer cust2 = new Customer();
        cust2.setId("2");
        cust2.setName("cust_2");
        store.put(this.bytesKey(cust2.getId()), bytesValue(cust2));


        final Customer cust3 = new Customer();
        cust3.setId("3");
        cust3.setName("cust_3");
        store.put(this.bytesKey(cust3.getId()), bytesValue(cust3));


        final KeyValueIterator<Bytes, byte[]> iterator = store.range(bytesKey(cust1.getId()), bytesKey(cust2.getId()));

        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();

            count++;
        }

        assertTrue(count > 0 );

    }

    private byte[] bytesValue(final Customer value) {

        ValueAndTimestamp<Customer> val =  ValueAndTimestamp.make(value,1L);

        return this.valueAndTimestampDes.serializer().serialize("", val);

    }

    private Bytes bytesKey(final String key) {
        return Bytes.wrap(key.getBytes());
    }
}
