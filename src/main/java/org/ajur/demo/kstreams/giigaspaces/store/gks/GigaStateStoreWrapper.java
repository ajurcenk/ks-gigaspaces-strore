package org.ajur.demo.kstreams.giigaspaces.store.gks;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class GigaStateStoreWrapper<K,V> implements GigaReadableStore<K,V> {


  private final StateStoreProvider provider;
  private final String storeName;
  private final QueryableStoreType<GigaReadableStore<K, V>> gigaStoreType;

  public GigaStateStoreWrapper(final StateStoreProvider provider,
                               final String storeName,
                               final QueryableStoreType<GigaReadableStore<K,V>> gigaStoreType) {

    this.provider = provider;
    this.storeName = storeName;
    this.gigaStoreType = gigaStoreType;
  }
  @Override
  public V read(K key) {

    List<GigaReadableStore<K,V>> stores = provider.stores(storeName, gigaStoreType);
    Optional<GigaReadableStore<K,V>> value = stores
        .stream()
        .filter(store -> store.read(key) != null)
        .findFirst();

    return value.map(store -> store.read(key)).orElse(null);
  }

}
