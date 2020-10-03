package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class GigaSpacesTransactionsTest {

    public static final String SPACE_DOC_TYPE = "test_doc";

    protected GigaSpace client;
    protected UrlSpaceConfigurer configurer;
    protected PlatformTransactionManager ptm;

    @Before
    public void setUp() throws Exception {

        // Create transaction manager
        ptm = new DistributedJiniTxManagerConfigurer().transactionManager();
        configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        client = new GigaSpaceConfigurer(configurer).transactionManager(ptm).clustered(true).gigaSpace();


        // Create test space document
        // register type
        final SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(SPACE_DOC_TYPE)
                .idProperty("key", false, SpaceIndexType.EQUAL)
                .addFixedProperty("key", String.class)
                .addFixedProperty("value", String.class)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);
    }

    @After
    public void clean() {

        configurer.close();
    }

    @Test
    public void testCommit() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());
        final TransactionStatus status = ptm.getTransaction(definition);
        try {

            final SpaceDocument spaceDocument = createSpaceDoc("1", "test-1");
            client.write(spaceDocument);

            this.ptm.commit(status);
        }
        catch (Exception ex) {

            this.ptm.rollback(status);
            throw  ex;
        }

    }

    @Test
    public void testRollback() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());
        final TransactionStatus status = ptm.getTransaction(definition);
        try {

            final SpaceDocument spaceDocument = createSpaceDoc("10", "test-10");
            client.write(spaceDocument);

            this.ptm.rollback(status);
        }
        catch (Exception ex) {

            this.ptm.rollback(status);
            throw  ex;
        }

    }


    @Test
    public void testPessimisticLockingWithTheSameKey() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

        // Start transaction
        final TransactionStatus status = ptm.getTransaction(definition);
        System.out.println("Transaction is started in the main thread");

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "1"), 500, ReadModifiers.EXCLUSIVE_READ_LOCK);

        // Update document
        spaceDoc.setProperty("value", "updated-" + System.currentTimeMillis());
        client.write(spaceDoc);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName);

            final DefaultTransactionDefinition definition_1 = new DefaultTransactionDefinition();
            definition_1.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

            // Start transaction
            final TransactionStatus status_1 = ptm.getTransaction(definition);

            System.out.println("Reading space document in thread " + threadName);
            final SpaceDocument spaceDoc_1 = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "1"), 1000000, ReadModifiers.EXCLUSIVE_READ_LOCK);
            System.out.println("Space document is found in thread " + threadName + " space doc: " + spaceDoc_1);

            System.out.println("Commit transaction in the thread " + threadName);
            this.ptm.commit(status_1);

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.ptm.commit(status);
        System.out.println("Transaction is committed in the main thread");

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testPessimisticLockingWithDifferentKey() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

        // Start transaction
        final TransactionStatus status = ptm.getTransaction(definition);
        System.out.println("Transaction is started in the main thread");

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "1"), 500, ReadModifiers.EXCLUSIVE_READ_LOCK);


        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName);

            final DefaultTransactionDefinition definition_1 = new DefaultTransactionDefinition();
            definition_1.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

            // Start transaction
            final TransactionStatus status_1 = ptm.getTransaction(definition);

            System.out.println("Reading space document in thread " + threadName);
            final SpaceDocument spaceDoc_1 = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "2"), 1000000, ReadModifiers.EXCLUSIVE_READ_LOCK);
            System.out.println("Space document is found in thread " + threadName + " space doc: " + spaceDoc_1);

            System.out.println("Commit transaction in the thread " + threadName);
            this.ptm.commit(status_1);

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.ptm.commit(status);
        System.out.println("Transaction is committed in the main thread");

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testConcurrentReadIfObjectIsNotExists() {

        final String key = "key-" + System.currentTimeMillis();
        final String value = "value-" + System.currentTimeMillis();

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

        // Start transaction
        final TransactionStatus status = ptm.getTransaction(definition);
        System.out.println("Transaction is started in the main thread");

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, key), 500, ReadModifiers.EXCLUSIVE_READ_LOCK);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName);

            final DefaultTransactionDefinition definition_1 = new DefaultTransactionDefinition();
            definition_1.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

            // Start transaction
            final TransactionStatus status_1 = ptm.getTransaction(definition_1);

            System.out.println("Reading space document in thread " + threadName);
            final SpaceDocument spaceDoc_1 = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, key), 2000, ReadModifiers.EXCLUSIVE_READ_LOCK);
            System.out.println("Space document is the thread " + threadName + " space doc: " + spaceDoc_1);

            System.out.println("Commit transaction in the thread " + threadName);
            this.ptm.commit(status_1);

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // Create document
        final SpaceDocument newDoc = createSpaceDoc(key,value);
        this.client.write(newDoc);

        this.ptm.commit(status);

        System.out.println("Transaction is committed in the main thread");

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testPessimisticLockingWithRead() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

        // Start transaction
        final TransactionStatus status = ptm.getTransaction(definition);
        System.out.println("Transaction is started in the main thread");

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "1"), 500, ReadModifiers.EXCLUSIVE_READ_LOCK);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Runnable task = () -> {

            String threadName = Thread.currentThread().getName();
            System.out.println("Starting transaction in the thread " + threadName);

            final DefaultTransactionDefinition definition_1 = new DefaultTransactionDefinition();
            definition_1.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());

            // Start transaction
            final TransactionStatus status_1 = ptm.getTransaction(definition);

            System.out.println("Reading space document in thread " + threadName);
            final SpaceDocument spaceDoc_1 = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, "1"), 1000000, ReadModifiers.EXCLUSIVE_READ_LOCK);
            System.out.println("Space document is found in thread " + threadName + " space doc: " + spaceDoc_1);

            System.out.println("Commit transaction in the thread " + threadName);
            this.ptm.commit(status_1);

            countDownLatch.countDown();
        };

        // Stat new transaction to access the same document
        Thread thread = new Thread(task);
        thread.start();

        // Pause main thread
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.ptm.commit(status);
        System.out.println("Transaction is committed in the main thread");

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private SpaceDocument createSpaceDoc(final String key, final String value) {

        final Map<String, Object> objectProps = new HashMap<>();
        objectProps.put("key",key);
        objectProps.put("value", value);

        final SpaceDocument spaceDoc = new SpaceDocument(SPACE_DOC_TYPE,objectProps);

        return  spaceDoc;
    }


}
