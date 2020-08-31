package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;

import java.util.HashMap;
import java.util.Map;

public class CustomerGigaSpacePropertiesExtractor implements GigaSpacePropertiesExtractor<Customer> {

    private static String PREFIX_DELIMITER =  "_";
    @Override
    public Map<String, SpacePropertyDescriptor> getSpaceProperties() {

        final Map<String,SpacePropertyDescriptor> props = new HashMap<>();

        final String[] indexedStringProp = new String[] {"id", "name"};

        for (String prop: indexedStringProp
             ) {

            final SpacePropertyDescriptor propDescr = new SpacePropertyDescriptor(this.getSpacePrefix()
                    + PREFIX_DELIMITER + prop
                    , String.class, true);

            props.put(propDescr.getName(), propDescr);
        }

        return props;
    }


    @Override
    public Map<String, Object> getSpaceValues(Customer object) {

        final Map<String,Object> values = new HashMap<>();

        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "id", object.getId());
        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "name", object.getName());

        return values;
    }

    @Override
    public String getSpacePrefix() {

        return "customer";
    }
}
