package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;


import java.util.List;

public class GigaSpacesChangeLoggingKeyValueBytesStore
        extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, byte[], byte[]>
        implements KeyValueStore<Bytes, byte[]> {

    StoreChangeLogger<Bytes, byte[]> changeLogger;

    public GigaSpacesChangeLoggingKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        super.init(context, root);
        final String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());
        changeLogger = new StoreChangeLogger<>(
                name(),
                context,
                new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));

        // if the inner store is an LRU cache, add the eviction listener to log removed record
        if (wrapped() instanceof MemoryLRUCache) {
            ((MemoryLRUCache) wrapped()).setWhenEldestRemoved((key, value) -> {
                // pass null to indicate removal
                log(key, null);
            });
        }
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value) {
        wrapped().put(key, value);
        log(key, value);
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] value) {
        final byte[] previous = wrapped().putIfAbsent(key, value);
        if (previous == null) {
            // then it was absent
            log(key, value);
        }
        return previous;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        wrapped().putAll(entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            log(entry.key, entry.value);
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        final byte[] oldValue = wrapped().delete(key);
        log(key, null);
        return oldValue;
    }

    @Override
    public byte[] get(final Bytes key) {
        return wrapped().get(key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return wrapped().range(from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return wrapped().all();
    }

    void log(final Bytes key,
             final byte[] value) {
        changeLogger.logChange(key, value);
    }
}