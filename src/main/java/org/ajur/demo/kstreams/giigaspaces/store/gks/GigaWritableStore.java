package org.ajur.demo.kstreams.giigaspaces.store.gks;

public interface GigaWritableStore<K,V> extends GigaReadableStore<K,V> {

  void write(K key, V value);

}
