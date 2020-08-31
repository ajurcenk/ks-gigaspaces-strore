package org.ajur.demo.kstreams.giigaspaces.store.gks;

import java.util.Map;

public interface GigaSpacePropertiesExtractor<T> {

    String getSpacePrefix();

    Map<String, SpacePropertyDescriptor> getSpaceProperties();

    Map<String, Object> getSpaceValues(final T object);
}
