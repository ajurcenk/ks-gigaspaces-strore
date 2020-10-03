package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import net.jini.core.lease.Lease;
import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.core.transaction.manager.DistributedJiniTxManagerConfigurer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.DefaultTransactionDefinition;


import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class GigaSpacesTransactionalStoreTest {

    public static final String SPACE_DOC_TYPE = "test_store_doc_v3";

    protected GigaSpace client;
    protected UrlSpaceConfigurer configurer;
    protected PlatformTransactionManager ptm;
    protected Serde<DiscountInfo> valueSerde;
    protected KeyValueStoreTestDriver<String, String> driver;
    protected KeyValueStore<String, String> store;
    protected InternalMockProcessorContext context;


    protected DiscountGigaSpacePropertiesExtractor propertiesExtractor = new DiscountGigaSpacePropertiesExtractor();

    @Before
    public void setUp() throws Exception {

        // Create transaction manager
        ptm = new DistributedJiniTxManagerConfigurer().transactionManager();
        configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        client = new GigaSpaceConfigurer(configurer).transactionManager(ptm).clustered(true).gigaSpace();

        this.driver = KeyValueStoreTestDriver.create(String.class, String.class);
        this.context = (InternalMockProcessorContext) this.driver.context();
        this.context.setTime(10L);
        this.context.setValueSerde(Serdes.String());
        this.context.setKeySerde(Serdes.String());
        this.store = this.createGigaSpacesKeyValueStore(this.context);


        // Create space type
        final SpaceTypeDescriptorBuilder docBuilder = new SpaceTypeDescriptorBuilder(SPACE_DOC_TYPE);
        propertiesExtractor.getSpaceProperties()
                .entrySet()
                .stream()
                .forEach(spaceProp -> {
                    docBuilder.addFixedProperty(spaceProp.getValue().getName(), spaceProp.getValue().getType());

                    // Add index
                    if (spaceProp.getValue().isIndexed()) {
                        docBuilder.addPropertyIndex(spaceProp.getValue().getName(), SpaceIndexType.EQUAL);
                    }
                });


        // Create test space document
        final SpaceTypeDescriptor typeDescriptor = docBuilder.idProperty("key_string", false)
                .addFixedProperty("key", byte[].class)
                .addFixedProperty("key_string", String.class)
                .addFixedProperty("value", byte[].class)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);

        valueSerde = SerdesFactory.from(DiscountInfo.class);
    }



    @After
    public void clean() {

        configurer.close();
    }


    protected KeyValueStore<String, String> createGigaSpacesKeyValueStore(ProcessorContext context) {


        final String storeName = "my-test-gigaspace-state-tran-store";

        final Map<String, String> changleLogCfg = new HashMap<>();

        // Create store with logging
        final GigaSpacesStateStoreStoreBuilder storeBuilder = new GigaSpacesStateStoreStoreBuilder(storeName,
                client, this.context.keySerde(),
                String.class, this.context.valueSerde(), String.class, null, ptm);

        // Enable logging
        storeBuilder.withLoggingEnabled(changleLogCfg);

        // Enable transactions
        storeBuilder.withSpaceTransactionsEnabled();

        final StateStore store = storeBuilder.build();
        store.init(context, store);

        return  (KeyValueStore) store;
    }



    @Test
    public void putAndGet() {


        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;

        // Start transaction
        final GigaSpacesTransaction tran = transStore.startNewTransaction();

        // put
        store.put(key, value);

        // get in transaction
        final String newValue = store.get(key);

        assertTrue(newValue.equals(value));

        // Commit transaction
        tran.commitTran();

    }

    @Test
    public void putAndGetWithUpdate() {


        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;
        final String updateValue = "test-" + System.currentTimeMillis() + "-new";;

        // Start transaction
        final GigaSpacesTransaction tran = transStore.startNewTransaction();

        // put
        store.put(key, value);

        // get in transaction
        final String newValue = store.get(key);

        assertTrue(newValue.equals(value));

        // Update
        store.put(key, updateValue);
        final String newValueV2 = store.get(key);

        assertTrue(newValueV2.equals(updateValue));

        // Commit transaction
        tran.commitTran();

    }

    @Test
    public void rollback() {


        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;

        // Start transaction
        final GigaSpacesTransaction tran = transStore.startNewTransaction();

        // put
        store.put(key, value);

        // Rollback transaction
        tran.rollback();

        // Start new transaction
        final GigaSpacesTransaction tran2 = transStore.startNewTransaction();

        // get in transaction
        final String newValue = store.get(key);

        assertNull(newValue);

        // Commit transaction
        tran2.commitTran();

    }


    @Test
    public void delete() {

        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;

        // Start transaction
        final GigaSpacesTransaction tran = transStore.startNewTransaction();

        // put
        store.put(key, value);

        // Read
        assertNotNull(store.get(key));

        // Commit transaction
        tran.commitTran();

        // Start new transaction
        final GigaSpacesTransaction tran2 = transStore.startNewTransaction();

        // Delete
        store.delete(key);

        // Read in transaction
        final String newValue = store.get(key);

        assertNull(newValue);

        // Commit transaction
        tran2.commitTran();

    }


    @Test
    public void testPessimisticLockingWithTheSameKey() {

        AtomicBoolean isTransactionCompletedInMainThread = new AtomicBoolean(false);

        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;
        final String updateValue = "test-" + System.currentTimeMillis() + "-update";;

        // Create test data
        final GigaSpacesTransaction insertTran = transStore.startNewTransaction();
        store.put(key, value);
        insertTran.commitTran();

        final GigaSpacesTransaction mainTran = transStore.startNewTransaction();

        System.out.println("Transaction is started in the main thread for key: " + key);

        final String readValue = store.get(key);
        assertNotNull(readValue);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName + " for key: " + key);


            // Start transaction
            final GigaSpacesTransaction childThreadTran = transStore.startNewTransaction();

            System.out.println("Start reading space document in thread " + threadName + " for key: " + key);
            final String docReadValue = store.get(key);

            assertNotNull(docReadValue);

            System.out.println("Space document in thread " + threadName + " for key: " + key + " " + docReadValue);

            assertNotNull(updateValue.equals(store.get(key)));
            assertTrue(isTransactionCompletedInMainThread.get());

            System.out.println("Commit transaction in the thread " + threadName + " for key: " + key);

            // Commit transaction
            childThreadTran.commitTran();

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // Update document
        System.out.println("Update document in the main thread for key: " + key);
        store.put(key, updateValue);

        mainTran.commitTran();
        isTransactionCompletedInMainThread.set(true);

        System.out.println("Transaction is committed in the main thread for key: " + key);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPessimisticLockingWithTheDifferentKey() {

        AtomicBoolean isTransactionCompletedInMainThread = new AtomicBoolean(false);

        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;
        final String updateValue = "test-" + System.currentTimeMillis() + "-update";;


        final String key1 = "key-" + System.currentTimeMillis() + 10;
        final String value1 = "value-" + System.currentTimeMillis() + 10;


        // Create test data
        final GigaSpacesTransaction insertTran = transStore.startNewTransaction();
        store.put(key, value);
        store.put(key1, value1);
        insertTran.commitTran();

        final GigaSpacesTransaction mainTran = transStore.startNewTransaction();

        System.out.println("Transaction is started in the main thread for key: " + key);

        final String readValue = store.get(key);
        assertNotNull(readValue);

        // Update document
        System.out.println("Update document in the main thread for key: " + key);
        store.put(key, updateValue);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName + " for key: " + key1);


            // Start transaction
            final GigaSpacesTransaction childThreadTran = transStore.startNewTransaction();

            System.out.println("Reading space document in thread " + threadName + " for key: " + key1);

            assertNotNull(store.get(key1));
            assertNotNull(updateValue.equals(store.get(key1)));

            // Don't need to wait for transaction in the main thread
            assertFalse(isTransactionCompletedInMainThread.get());

            System.out.println("Commit transaction in the thread " + threadName + " for key: " + key1);

            // Commit transaction
            childThreadTran.commitTran();

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        mainTran.commitTran();
        isTransactionCompletedInMainThread.set(true);

        System.out.println("Transaction is committed in the main thread for key: " + key);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    @Test
    public void testPessimisticLockingKeyIsNotExists() {

        AtomicBoolean isTransactionCompletedInMainThread = new AtomicBoolean(false);

        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        final String key = "key-" + System.currentTimeMillis();
        final String value = "test-" + System.currentTimeMillis();;
        final String updateValue = "test-" + System.currentTimeMillis() + "-update";;

        final GigaSpacesTransaction mainTran = transStore.startNewTransaction();

        System.out.println("Transaction is started in the main thread for key: " + key);

        final String readValue = store.get(key);
        assertNull(readValue);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName + " for key: " + key);


            // Start transaction
            final GigaSpacesTransaction childThreadTran = transStore.startNewTransaction();

            System.out.println("Reading space document in thread " + threadName + " for key: " + key);

            //assertNotNull(transStore.get(Bytes.wrap(key.getBytes())));

            System.out.println("Read operation is completed the thread in thread " + threadName + " for key: " + key);

            assertNotNull(store.get(key));

            assertNotNull(updateValue.equals(store.get(key)));
            assertTrue(isTransactionCompletedInMainThread.get());

            System.out.println("Commit transaction in the thread " + threadName + " for key: " + key);

            // Commit transaction
            childThreadTran.commitTran();

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(100L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Create document
        System.out.println("Create document in the main thread for key: " + key);
        store.put(key, updateValue);

        isTransactionCompletedInMainThread.getAndSet(true);
        mainTran.commitTran();

        System.out.println("Transaction is committed in the main thread for key: " + key);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void getAllWithConcurrency() {

        final AtomicBoolean isTransactionCompletedInMainThread = new AtomicBoolean(false);
        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);

        System.out.println("Starting transaction in main thread ");
        final GigaSpacesTransaction mainTran = transStore.startNewTransaction();

        final String currentProcessingRecKey = "abcd";
        final SpaceDocument currentProcessingRecSpaceDoc = createSpaceDoc(currentProcessingRecKey, Thread.currentThread().getName());

        // Create test data
        for (int i = 0; i < 3; i++) {

            final String key = String.format("key-%d", System.currentTimeMillis());

            try {
                Thread.sleep(100l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            final String value = String.format("test-%d", i);
            store.put(key, value);
        }

        // Add the lock
        this.client.write(currentProcessingRecSpaceDoc, Lease.FOREVER, 10000L, WriteModifiers.UPDATE_OR_WRITE);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName);

            // Start transaction
            final GigaSpacesTransaction childThreadTran = transStore.startNewTransaction();

            final SpaceDocument currentProcessingRecSpaceDoc2 = createSpaceDoc(currentProcessingRecKey, Thread.currentThread().getName());
            this.client.write(currentProcessingRecSpaceDoc2, Lease.FOREVER, 10000L, WriteModifiers.UPDATE_OR_WRITE );

            System.out.println("Creating iterator in thread " + threadName);

            final KeyValueIterator<String, String> iterator = store.all();

            System.out.println("Iterating over docs in thread " + threadName);

            assertTrue(isTransactionCompletedInMainThread.get());

            int count = 0;
            while (iterator.hasNext()) {

                iterator.next();
                count++;
            }
            assertTrue(count >0 );

            System.out.println("Commit transaction in the thread " + threadName);

            // Commit transaction
            childThreadTran.commitTran();

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();


        System.out.println("Creating iterator in the main thread" );
        final KeyValueIterator<String, String> iterator = store.all();

        System.out.println("Iterating over docs the main thread" );
        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();
            count++;
        }

        System.out.println("Commit transaction in the main thread" );
        mainTran.commitTran();
        isTransactionCompletedInMainThread.set(true);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(count >0 );

    }


    @Test
    public void getAll() {

        final AtomicBoolean isTransactionCompletedInMainThread = new AtomicBoolean(false);
        final KeyValueStore<String, String> store = this.store;
        final GigaSpacesTransactionalStateStore transStore = getTranStore(store);
        final GigaSpacesTransaction mainTran = transStore.startNewTransaction();

        for (int i = 0; i < 3; i++) {

            final String key = String.format("key-%d", System.currentTimeMillis());

            try {
                Thread.sleep(100l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            final String value = String.format("test-%d", i);
            store.put(key, value);
        }


        final KeyValueIterator<String, String> iterator = store.all();

        int count = 0;
        while (iterator.hasNext()) {

            iterator.next();
            count++;
        }

        mainTran.commitTran();

        assertTrue(count >0 );

    }

    @Test
    public void testInsert() {

        final DiscountInfo discountInfo = new DiscountInfo();
        discountInfo.setCustomerCardNo("1");
        discountInfo.setTotalAmount(100.00);

        SpaceDocument spaceDocument = createSpaceDoc("1", discountInfo);
        client.write(spaceDocument);
    }


    @Test
    public void testReadById() {

        final String key = "1";
        final String id = convertToStringKey(key);

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, id));

        assertNotNull(spaceDoc);
    }


    @Test
    public void testUpdate() {

        final String key = "1";
        final String id = convertToStringKey(key);

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, id));

        assertNotNull(spaceDoc);

        // Update existing doc
        spaceDoc.setProperty("totalAmount", 120.00d);

        client.write(spaceDoc);
    }


    private SpaceDocument createSpaceDoc(final String key, DiscountInfo discountInfo) {

        final Map<String, Object> objectProps = new HashMap<>();

        final byte[] bytesKey = Serdes.String().serializer().serialize("",key);
        final String encodedKey = Base64.getEncoder().encodeToString(bytesKey);

        objectProps.put("key", bytesKey);
        objectProps.put("key_string", encodedKey);
        objectProps.put("value", this.valueSerde.serializer().serialize("", discountInfo));
        objectProps.putAll(this.propertiesExtractor.getSpaceValues(discountInfo));

        final SpaceDocument spaceDoc = new SpaceDocument(SPACE_DOC_TYPE,objectProps);

        return  spaceDoc;
    }

    private SpaceDocument createSpaceDoc(final String key, final String value) {

        final byte[] bytesKey = Serdes.String().serializer().serialize("",key);
        final String encodedKey = Base64.getEncoder().encodeToString(bytesKey);

        final Map<String, Object> objectProps = new HashMap<>();
        objectProps.put("key",key);
        objectProps.put("key_string", encodedKey);
        objectProps.put("value", value);

        final SpaceDocument spaceDoc = new SpaceDocument(SPACE_DOC_TYPE,objectProps);

        return  spaceDoc;
    }


    private String convertToStringKey(final String id) {

        final byte[] bytesKey = Serdes.String().serializer().serialize("",id);
        final String encodedKey = Base64.getEncoder().encodeToString(bytesKey);

        return  encodedKey;
    }

    private GigaSpacesTransactionalStateStore getTranStore(StateStore currentStore) {


        while(currentStore != null) {

            if (currentStore instanceof WrappedStateStore) {

                currentStore = ((WrappedStateStore) currentStore).wrapped();
                continue;
            }

            break;
        }

         return  (GigaSpacesTransactionalStateStore) currentStore;
    }


}
