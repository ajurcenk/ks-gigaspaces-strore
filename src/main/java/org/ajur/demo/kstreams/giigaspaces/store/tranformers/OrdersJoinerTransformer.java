package org.ajur.demo.kstreams.giigaspaces.store.tranformers;


import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;
import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderCustomerJoinResult;
import org.ajur.demo.kstreams.giigaspaces.store.model.OrderWrapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.openspaces.core.GigaSpace;

public class OrdersJoinerTransformer  implements Transformer<String, OrderWrapper, KeyValue<String, OrderCustomerJoinResult>> {

    private ProcessorContext context;
    private GigaSpace client;
    private Serde<Customer> customerSerde;


    public OrdersJoinerTransformer(GigaSpace client, final Serde<Customer> customerSerde) {
        this.client = client;
        this.customerSerde = customerSerde;
    }

    @Override
    public void init(ProcessorContext context) {

        this.context = context;

    }

    @Override
    public KeyValue<String, OrderCustomerJoinResult> transform(String key, OrderWrapper value) {

        // Get customer by customer ID
        final Customer customer = this.readStrongTypedValueByKey(value.getOrder().getCustomerId());

        final OrderCustomerJoinResult joinResult = new OrderCustomerJoinResult();
        joinResult.setCustomer(customer);
        joinResult.setOrder(value);

        final  KeyValue<String, OrderCustomerJoinResult> keyVal = new KeyValue(key, joinResult);

        return keyVal;
    }

    @Override
    public void close() {

    }

    public Customer readStrongTypedValueByKey(String key) {

        final SQLQuery<SpaceDocument> query =
                new SQLQuery<SpaceDocument>("customers", "strongTypedKey = ?");

        query.setParameter(1, key);

        final SpaceDocument result = this.client.read(query);

        if (result == null) {

            return null;
        }

        final byte[] valueBytes = result.getProperty("value");
        final ValueAndTimestampSerde<Customer> valueAndTimestampDes = new ValueAndTimestampSerde<>(this.customerSerde);
        final ValueAndTimestamp<Customer> valueAndTimestamp = valueAndTimestampDes.deserializer().deserialize("", valueBytes);

        Customer value = valueAndTimestamp.value();

        return value;
    }
}
