package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.model.Invoice;

import java.util.HashMap;
import java.util.Map;

public class DiscountGigaSpacePropertiesExtractor implements GigaSpacePropertiesExtractor<DiscountInfo> {

    private static String PREFIX_DELIMITER =  "_";
    @Override
    public Map<String, SpacePropertyDescriptor> getSpaceProperties() {

        final Map<String,SpacePropertyDescriptor> props = new HashMap<>();

        final String[] indexedPropNames = new String[] {"customerCardNo", "totalAmount", "earnedLoyaltyPoints", "totalLoyaltyPoints"};
        final Class[] indexedPropTypes = new Class[] {String.class, Double.class, Double.class, Double.class};

        for (int i = 0; i < indexedPropNames.length; i++) {

            final SpacePropertyDescriptor propDescr = new SpacePropertyDescriptor(this.getSpacePrefix()
                    + PREFIX_DELIMITER + indexedPropNames[i]
                    , indexedPropTypes[i], true);

            props.put(propDescr.getName(), propDescr);
        }


        return props;
    }


    @Override
    public Map<String, Object> getSpaceValues(DiscountInfo object) {

        final Map<String,Object> values = new HashMap<>();

        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "customerCardNo", object.getCustomerCardNo());
        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "totalAmount", object.getTotalAmount());
        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "earnedLoyaltyPoints", object.getEarnedLoyaltyPoints());
        values.put(this.getSpacePrefix() + PREFIX_DELIMITER + "totalLoyaltyPoints", object.getTotalLoyaltyPoints());

        return values;
    }

    @Override
    public String getSpacePrefix() {

        return "discount";
    }
}
