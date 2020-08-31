package org.ajur.demo.kstreams.giigaspaces.store.gks;

import java.util.Map;
import org.apache.kafka.streams.state.StoreBuilder;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

public class GigaStoreBuilder<K,V> implements StoreBuilder<GigaStateStore> {
  private final String storeName;
  private GigaSpace gigaspaces;
  private Map<String, String> config;
  Class<K> Obj1;
  Class<V> Obj2;

  public GigaStoreBuilder(String storeName,Class<K> Obj1,Class<V> Obj2) {
    this.storeName = storeName;
    this.Obj1 = Obj1;
    this.Obj2 = Obj2;
  }

  public GigaStoreBuilder<K,V> gigaspaces(GigaSpace gigaspaces){
    this.gigaspaces = gigaspaces;
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withCachingEnabled() {
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withCachingDisabled() {
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore> withLoggingEnabled(Map<String, String> config) {
    this.config = config;
    return this;
  }

  @Override
  public StoreBuilder<GigaStateStore>  withLoggingDisabled() {
    return this;
  }

  @Override
  public GigaStateStore<K,V> build() {

    if (this.gigaspaces != null) {

      // Use builder cache
      final GigaStateStore<K,V> gigaspacesStore = new GigaStateStore<K ,V>(gigaspaces, storeName,Obj1, Obj2);

      return  gigaspacesStore;
    }

    this.gigaspaces = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer(storeName)).create();

    return new GigaStateStore<K ,V>(gigaspaces, storeName,Obj1, Obj2);
  }

  @Override
  public Map<String, String> logConfig() {
    return config;
  }

  @Override
  public boolean loggingEnabled() {
    return false;
  }

  @Override
  public String name() {
    return this.storeName;
  }
}
