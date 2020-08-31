package org.ajur.demo.kstreams.giigaspaces.store.gks;

import java.util.List;

public interface GigaReadableStore<K,V>  {

  V read(K key);

}
