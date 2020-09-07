package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.client.iterator.SpaceIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.j_spaces.core.client.SQLQuery;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.openspaces.core.GigaSpace;

import java.rmi.RemoteException;
import java.util.*;

public class GigaSpacesKeyValueStoreOld<K,V>  implements KeyValueStore<Bytes, byte[]> {


    private GigaSpace client;
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
    protected GigaSpacesKeyValueStoreOld(GigaSpace client, String storeName, final String typeName ) {

        this.storeName = storeName;
        this.typeName = typeName;
        this.client = client;
    }

    public GigaSpacesKeyValueStoreOld(GigaSpace client, String storeName) {

        this.storeName = storeName;
        this.typeName = Utils.convertToValidJavaName(storeName);
        this.client = client;
    }

    public GigaSpacesKeyValueStoreOld(GigaSpace client, String storeName,
                                      final Class<K> keyType, final  Class<V> valueType,
                                      final Serde<K> keySerde, final Serde<V> valueSerde

    ) {

        this.storeName = storeName;
        this.typeName = Utils.convertToValidJavaName(storeName);
        this.client = client;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public GigaSpacesKeyValueStoreOld(GigaSpace client, String storeName,
                                      final Class<K> keyType, final  Class<V> valueType,
                                      final Serde<K> keySerde, final Serde<V> valueSerde,
                                      final GigaSpacePropertiesExtractor<V> spaceValueExtractor

    ) {
        this(client,storeName,keyType,valueType,keySerde,valueSerde);
        this.spaceValueExtractor = spaceValueExtractor;
    }

    @Override
    public void init(ProcessorContext processorContext, StateStore root) {

        context = processorContext;

        this.typeName = Utils.convertToValidJavaName(storeName);


        final SpaceTypeDescriptorBuilder  docBuilder = new SpaceTypeDescriptorBuilder(this.typeName)
               // .idProperty("key", false)
                .addFixedProperty("key", byte[].class)
                .addFixedProperty("value", byte[].class);


        if (this.valueSerde != null &&
                this.keySerde != null &&
            this.keyType != null &&
                this.valueType != null) {

            docBuilder.addFixedProperty("strongTypedKey", this.keyType);
            // Add index on strong typed key property
            docBuilder.addPropertyIndex("strongTypedKey", SpaceIndexType.EQUAL);

            docBuilder.addFixedProperty("strongTypedValue", this.valueType);


            if (this.spaceValueExtractor !=null) {

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
            context.register(root, (key, value) -> put(Bytes.wrap(key), value));
        }

    }


    @Override
    public void put(Bytes key, byte[] value) {

        // TODO Add update support
        if (this.getDoc(key.get()) != null) {

            return;
        }

        final Map<String, Object> objectProps = new HashMap<>();
        objectProps.put("key", key.get());
        objectProps.put("value", value);

        if (this.valueSerde != null &&
                this.keySerde != null &&
                this.keyType != null &&
                this.valueType != null) {

            // Deserialize to support strong type
            final ValueAndTimestampSerde<V> valueAndTimestampDes = new ValueAndTimestampSerde<V>(this.valueSerde);
            final ValueAndTimestamp<V> valueAndTimestamp = valueAndTimestampDes.deserializer().deserialize("", value);

            final V strongTypedValue = valueAndTimestamp.value();
            final K strongTypedKey = this.keySerde.deserializer().deserialize("", key.get());

            // Stored  deserialized  key
            objectProps.put("strongTypedKey", strongTypedKey);

            if (this.spaceValueExtractor != null) {

                objectProps.putAll(this.spaceValueExtractor.getSpaceValues(strongTypedValue));
            }


        }

        final SpaceDocument spaceDoc = new SpaceDocument(this.typeName,objectProps);

        this.client.write(spaceDoc);

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

        // TODO Ask how to delete document from space

        final SpaceDocument document = this.getDoc(key.get());

        if (document != null) {

            final byte[] oldValue = document.getProperty("value");
            this.client.take(document);

            return oldValue;
        }

        // TODO or throw am error
        return null;
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

            while(iter.hasNext()) {

                final  KeyValue<Bytes, byte[]> keyVal = iter.next();
                map.put(keyVal.key, keyVal.value);
            }
        }
        finally {

            if (iter != null) {

                iter.close();
            }
        }

        final Map<Bytes, byte[]> subMap = map.subMap(from, to);

        return  new SpaceInMemoryKeyValueIterator(subMap);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {

        final  KeyValueIterator<Bytes, byte[]> iter = new SpaceKeyValueIterator();

        return iter;
    }

    @Override
    public long approximateNumEntries() {
        // TODO Fix it
        return  10;
    }

    private SpaceDocument getDoc(byte[] key) {

        // TODO byte[] key is not working using, use SQL
        //final SpaceDocument spaceDocument = client.readById(new IdQuery<>(typeName, key));

        final K strongTypedKey = this.keySerde.deserializer().deserialize("", key);

        final SpaceDocument spaceDocument = this.readByKey(strongTypedKey);

        return spaceDocument;
    }

    public SpaceDocument readByKey(K key) {

        final SQLQuery<SpaceDocument> query =
                new SQLQuery<SpaceDocument>(this.typeName, "strongTypedKey = ?");

        query.setParameter(1, key);

        final SpaceDocument result = this.client.read(query);

        return result;
    }

    public V readStrongTypedValueByKey(K key) {

        final SQLQuery<SpaceDocument> query =
                new SQLQuery<SpaceDocument>(this.typeName, "strongTypedKey = ?");

        query.setParameter(1, key);

        final SpaceDocument result = this.client.read(query);
        final byte[] valueBytes = result.getProperty("value");
        final ValueAndTimestampSerde<V> valueAndTimestampDes = new ValueAndTimestampSerde<V>(this.valueSerde);
        final ValueAndTimestamp<V> valueAndTimestamp = valueAndTimestampDes.deserializer().deserialize("", valueBytes);

        V value = valueAndTimestamp.value();

        return value;
    }


    public SpaceDocument querySingle(final String sqlExpr, Object[] params) {

        final SQLQuery<SpaceDocument> query =
                new SQLQuery<SpaceDocument>(this.typeName, sqlExpr);

        for (int i = 0; i < params.length; i++) {

            query.setParameter((i+1), params[i]);
        }

        final SpaceDocument result = this.client.read(query);

        return result;
    }



    /**
     * Space KeyValue iterator
     */
    private class SpaceKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {

        final SpaceIterator<SpaceDocument> spaceIter;

        private SpaceKeyValueIterator() {

            // Create space iterator
            final SQLQuery<SpaceDocument> query =
                    new SQLQuery<SpaceDocument>(typeName, "strongTypedKey != ?");

            // TODO Use random key
            query.setParameter(1, "");

            this.spaceIter = client.iterator(query);
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


        private SpaceInMemoryKeyValueIterator(final Map<Bytes, byte[]> map ) {
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

}
