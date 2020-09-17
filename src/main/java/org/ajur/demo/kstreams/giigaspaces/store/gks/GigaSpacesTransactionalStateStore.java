package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeByIdsResult;
import com.gigaspaces.client.iterator.SpaceIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.client.SQLQuery;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.openspaces.core.GigaSpace;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.rmi.RemoteException;
import java.util.*;

/**
 * Supports spaces transactions
 *
 * @param <K>
 * @param <V>
 */
public class GigaSpacesTransactionalStateStore<K, V> implements KeyValueStore<Bytes, byte[]> {


    public static final String STRONG_TYPED_KEY = "strongTypedKey";
    public static final String STRONG_TYPED_VALUE = "strongTypedValue";
    public static final String KEY_STRING = "key_string";

    private GigaSpace client;

    /**
     * Transactions support
     */
    private PlatformTransactionManager ptm;
    private ReadModifiers isolationLevel = ReadModifiers.EXCLUSIVE_READ_LOCK;
    private Long transactionTimeout = 500l;

    private ProcessorContext context;
    private String storeName;
    private String typeName;

    private Class<K> keyType;
    private Class<V> valueType;

    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    private GigaSpacePropertiesExtractor<V> spaceValueExtractor;

    /**
     * For testing
     *
     * @param client
     * @param storeName
     * @param typeName
     */
    protected GigaSpacesTransactionalStateStore(GigaSpace client, String storeName, final String typeName) {

        this.storeName = storeName;
        this.typeName = typeName;
        this.client = client;
    }


    public GigaSpacesTransactionalStateStore(GigaSpace client, String storeName,
                                             final Class<K> keyType, final Class<V> valueType,
                                             final Serde<K> keySerde, final Serde<V> valueSerde,
                                             final PlatformTransactionManager ptm

    ) {

        this.storeName = storeName;
        this.typeName = Utils.convertToValidJavaName(storeName);
        this.client = client;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.ptm = ptm;
    }

    public GigaSpacesTransactionalStateStore(GigaSpace client, String storeName,
                                             final Class<K> keyType, final Class<V> valueType,
                                             final Serde<K> keySerde, final Serde<V> valueSerde,
                                             final GigaSpacePropertiesExtractor<V> spaceValueExtractor,
                                             PlatformTransactionManager ptm

    ) {
        this(client, storeName, keyType, valueType, keySerde, valueSerde, ptm);
        this.spaceValueExtractor = spaceValueExtractor;
    }

    @Override
    public void init(ProcessorContext processorContext, StateStore root) {

        context = processorContext;

        this.typeName = Utils.convertToValidJavaName(storeName);


        final SpaceTypeDescriptorBuilder docBuilder = new SpaceTypeDescriptorBuilder(this.typeName)
                // .idProperty("key", false)
                .addFixedProperty("key", byte[].class)
                .addFixedProperty("value", byte[].class);


        if (this.valueSerde != null &&
                this.keySerde != null &&
                this.keyType != null &&
                this.valueType != null) {

            // string key is space ID property
            docBuilder.idProperty(KEY_STRING, false);
            docBuilder.addFixedProperty(STRONG_TYPED_KEY, this.keyType);
            docBuilder.addPropertyIndex(STRONG_TYPED_KEY, SpaceIndexType.EQUAL);
            docBuilder.addFixedProperty(STRONG_TYPED_VALUE, this.valueType);

            if (this.spaceValueExtractor != null) {

                // Add object properties to space
                this.spaceValueExtractor.getSpaceProperties()
                        .entrySet()
                        .stream()
                        .forEach(spaceProp -> {
                            docBuilder.addFixedProperty(spaceProp.getValue().getName(), spaceProp.getValue().getType());

                            // Add index
                            if (spaceProp.getValue().isIndexed()) {
                                docBuilder.addPropertyIndex(spaceProp.getValue().getName(), SpaceIndexType.EQUAL);
                            }
                        });
            }

        }

        // register type
        SpaceTypeDescriptor typeDescriptor = docBuilder.create();
        client.getTypeManager().registerTypeDescriptor(typeDescriptor);

        // Store wrapper support
        if (root != null) {

            // Add restore logic
            context.register(root, (key, value) -> {

                // Not transactional restore
                putNoTransactions(Bytes.wrap(key), value);

            });
        }

    }

    @Override
    public void put(Bytes key, byte[] value) {

        final SpaceDocument existingDoc = this.getDoc(key.get());
        final SpaceDocument spaceDoc = (existingDoc == null) ? createDoc(key, value) : populateDoc(value,existingDoc);

        this.client.write(spaceDoc);

    }


    public void putNoTransactions(Bytes key, byte[] value) {

        final SpaceDocument existingDoc = this.getDocNoTransactions(key.get());
        final SpaceDocument spaceDoc = (existingDoc == null) ? createDoc(key, value) : populateDoc(value,existingDoc);

        this.client.write(spaceDoc);

    }

    private SpaceDocument createDoc(Bytes key, byte[] value) {

            final Map<String, Object> objectProps = new HashMap<>();

            objectProps.put("key", key.get());
            objectProps.put("value", value);

            if (this.valueSerde != null &&
                    this.keySerde != null &&
                    this.keyType != null &&
                    this.valueType != null) {

                final V strongTypedValue = this.valueSerde.deserializer().deserialize("", value);
                final K strongTypedKey = this.keySerde.deserializer().deserialize("", key.get());
                final String encodedKey = Base64.getEncoder().encodeToString(key.get());

                objectProps.put(KEY_STRING, encodedKey);
                objectProps.put(STRONG_TYPED_KEY, strongTypedKey);

                if (this.spaceValueExtractor != null) {

                    objectProps.putAll(this.spaceValueExtractor.getSpaceValues(strongTypedValue));
                } else {

                    objectProps.put(STRONG_TYPED_VALUE, strongTypedValue);
                }
            }

            final SpaceDocument spaceDoc = new SpaceDocument(this.typeName, objectProps);

            return spaceDoc;

    }

    private SpaceDocument populateDoc(byte[] value, final SpaceDocument spaceDoc) {

        // Set value
        spaceDoc.setProperty("value", value);

        // Set extracted properties
        if (this.valueSerde != null &&
                this.keySerde != null &&
                this.keyType != null &&
                this.valueType != null) {

            final V strongTypedValue = this.valueSerde.deserializer().deserialize("", value);
            if (this.spaceValueExtractor != null) {

                this.spaceValueExtractor.getSpaceValues(strongTypedValue)
                        .entrySet()
                        .stream()
                        .forEach(entry ->spaceDoc.setProperty(entry.getKey(), entry.getValue()));
            } else {

                spaceDoc.setProperty(STRONG_TYPED_VALUE, strongTypedValue);
            }
        } // if

        return spaceDoc;
    }


    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {

        final byte[] originalValue = get(key);

        if (originalValue == null) {

            put(key, value);
        }

        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {

        // TODO Use writeMultiple method
        entries.forEach(bytesKeyValue -> {

            this.put(bytesKeyValue.key, bytesKeyValue.value);
        });
    }

    @Override
    public byte[] delete(Bytes key) {

        final String id = Base64.getEncoder().encodeToString(key.get());

        SpaceDocument doc = client.takeById(new IdQuery<SpaceDocument>(this.typeName, id));

        final byte[] deletedValue = doc.getProperty("value");

        return  deletedValue;
    }

    @Override
    public String name() {

        return this.storeName;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        try {

            client.getSpace().ping();

            return true;

        } catch (RemoteException e) {

            e.printStackTrace();
        }

        return false;
    }

    @Override
    public byte[] get(Bytes key) {

        final SpaceDocument spaceDocument = this.getDoc(key.get());

        if (spaceDocument != null) {

            byte[] value = spaceDocument.getProperty("value");

            return value;
        }

        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {

        if (from.compareTo(to) > 0) {

            return new SpaceEmptyKeyValueIterator();
        }

        final NavigableMap<Bytes, byte[]> map = new TreeMap<>();
        KeyValueIterator<Bytes, byte[]> iter = null;

        try {

            iter = all();

            while (iter.hasNext()) {

                final KeyValue<Bytes, byte[]> keyVal = iter.next();
                map.put(keyVal.key, keyVal.value);
            }
        } finally {

            if (iter != null) {

                iter.close();
            }
        }

        final Map<Bytes, byte[]> subMap = map.subMap(from, to);

        return new SpaceInMemoryKeyValueIterator(subMap);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {

        final KeyValueIterator<Bytes, byte[]> iter = new SpaceKeyValueIterator();

        return iter;
    }

    @Override
    public long approximateNumEntries() {
        // TODO Fix it
        return 10;
    }

    private SpaceDocument getDoc(byte[] key) {

        final String id = Base64.getEncoder().encodeToString(key);

        // Read using isolation level
        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(this.typeName, id), transactionTimeout, isolationLevel);

        return spaceDoc;
    }

    private SpaceDocument getDocNoTransactions(byte[] key) {

        final String id = Base64.getEncoder().encodeToString(key);

        // Read using isolation level
        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(this.typeName, id));

        return spaceDoc;
    }


    /**
     * Space KeyValue iterator
     */
    private class SpaceKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {

        final SpaceIterator<SpaceDocument> spaceIter;

        private SpaceKeyValueIterator() {

            // Create space iterator
            // TODO Incorrect parameter: isolationLevel - no supported. Fix it.
            final SQLQuery<SpaceDocument> query =
                    new SQLQuery<SpaceDocument>(typeName, "strongTypedKey != ?", transactionTimeout, isolationLevel);

            // TODO Use random key
            query.setParameter(1, "");

            //this.spaceIter = client.iterator(query, transactionTimeout, isolationLevel);

            this.spaceIter = client.iterator(query,10000, isolationLevel);

        }

        @Override
        public boolean hasNext() {

            return this.spaceIter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {

            final SpaceDocument spaceDoc = this.spaceIter.next();
            final byte[] value = spaceDoc.getProperty("value");

            // TODO Do we need to wrap byte[]
            final Bytes key = Bytes.wrap(spaceDoc.getProperty("key"));

            return new KeyValue<>(key, value);
        }

        @Override
        public void close() {

            this.spaceIter.close();
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }

    }

    private class SpaceInMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {

        private final Iterator<Bytes> iter;
        private Map<Bytes, byte[]> map;


        private SpaceInMemoryKeyValueIterator(final Map<Bytes, byte[]> map) {
            this.map = map;
            this.iter = this.map.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final Bytes key = iter.next();
            return new KeyValue<>(key, map.get(key));
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }

    private static class SpaceEmptyKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

        @Override
        public void close() {
        }

        @Override
        public K peekNextKey() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public KeyValue<K, V> next() {
            throw new NoSuchElementException();
        }

    }

    public String getSpaceTypeName() {

        return typeName;
    }

    public GigaSpace getSpaceClient() {

        return client;
    }

    public GigaSpacesTransaction startNewTransaction() {

        final DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(Propagation.REQUIRES_NEW.ordinal());
        final TransactionStatus status = ptm.getTransaction(definition);

        final GigaSpacesTransaction tran = new GigaSpacesTransaction(ptm, status);

        return tran;
    }

}
