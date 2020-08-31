package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.rmi.RemoteException;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.openspaces.core.GigaSpace;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;



public class GigaStateStore<K,V> implements StateStore, GigaWritableStore<K, V> {

    private Class<K> Obj1;
    private Class<V> Obj2;

    private final String storeName;
    private final String typeName;

    private GigaSpace client;
    private ProcessorContext context;

    public GigaStateStore(GigaSpace client, String storeName, Class<K> Obj1,Class<V> Obj2) {
        this.Obj1 = Obj1;
        this.Obj2 = Obj2;
        this.storeName = storeName;
        this.typeName = Utils.convertToValidJavaName(storeName);
        this.client = client;
    }

    @Override // GigaReadableStore
    public V read(K key) {
        if (key == null) {
            return null;
        }
        SpaceDocument spaceDocument = client.readById(new IdQuery<>(typeName, key));
        if( spaceDocument != null ) {
            V value = spaceDocument.getProperty("value");
            return value;
        }
        else { // no document found
            return null;
        }
    }

    @Override // GigaWritableStore
    public void write(K key, V value) {

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("key", key);
        properties.put("value", value);

        SpaceDocument spaceDocument = new SpaceDocument(this.typeName, properties);

        client.write(spaceDocument);

    }

    @Override // StateStore
    public String name() {
        return this.storeName;
    }

    @Override // StateStore //aaa
    public void init(ProcessorContext processorContext, StateStore stateStore) {

        context = processorContext;
        // register type
        SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(this.typeName)
                .idProperty("key", false, SpaceIndexType.EQUAL)
                .addFixedProperty("key", Obj1)
                .addFixedProperty("value", Obj2)
                .addPropertyIndex("content", SpaceIndexType.EQUAL)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);

        context.register(this, (key, value) -> {

            System.out.println("");
        });
    }

    @Override //StateStore
    public void flush() {
    /*
      Definition of a flush, for which there is no GigaSpace equivalent
     */
    }

    @Override // StateStore
    public void close() {
    /*
      We don't have a GigaSpace.close equivalent, unless we use internal API
    */
    }

    @Override //StateStore
    public boolean persistent() {
        return true;
    }

    @Override //StateStore
    public boolean isOpen() {
            try {
            client.getSpace().ping();
            return true;
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return false;
    }

}

