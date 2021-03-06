package org.ajur.demo.kstreams.giigaspaces.store.tranformers;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;
import org.ajur.demo.kstreams.giigaspaces.store.gks.GigaSpacesStateStore;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfoWrapper;
import org.ajur.demo.kstreams.giigaspaces.store.model.Invoice;
import org.ajur.demo.kstreams.giigaspaces.store.model.InvoiceWrapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import static org.openspaces.extensions.QueryExtension.*;

import java.util.Objects;

public class DiscountLogicProcessor  implements Transformer<String, InvoiceWrapper, KeyValue<String, DiscountInfoWrapper>> {

    public static String STORE_NAME = "discount-store";

    private ProcessorContext context;

    private GigaSpacesStateStore<String,DiscountInfo> spacesStore;
    private KeyValueStore<String, DiscountInfo> store;

    public DiscountLogicProcessor() {

    }

    @Override
    public void init(ProcessorContext context) {

        this.context = context;



        this.store = (KeyValueStore<String, DiscountInfo>) this.context.getStateStore(STORE_NAME);

        StateStore currentStore = this.context.getStateStore(STORE_NAME);

        // TODO Use utility class to get the spaces store
        while(currentStore != null) {

            if (currentStore instanceof WrappedStateStore) {

                currentStore = ((WrappedStateStore) currentStore).wrapped();
                continue;
            }

            break;
        }

        this.spacesStore = (GigaSpacesStateStore) currentStore;
    }

    @Override
    public KeyValue<String, DiscountInfoWrapper> transform(String key, InvoiceWrapper invoice) {


        // Get customer discount
        final String custNo = invoice.getInvoice().getCustomerNo();
        final DiscountInfo currentDiscount = this.store.get(custNo);

        final DiscountInfoWrapper discountInfoWrapper = new DiscountInfoWrapper();
        // Keep start time
        discountInfoWrapper.setStartTime(invoice.getStartTime());

        KeyValue<String, DiscountInfoWrapper> retVal = null;


        if (currentDiscount == null) {

            // Discount not found
            final DiscountInfo discountInfo = new DiscountInfo();
            discountInfo.setCustomerCardNo(invoice.getInvoice().getCustomerNo());
            discountInfo.setEarnedLoyaltyPoints(1d);
            discountInfo.setTotalAmount(invoice.getInvoice().getTotalAmount());
            discountInfo.setTotalLoyaltyPoints(1d);

            this.store.put(custNo, discountInfo);

            discountInfoWrapper.setDiscountInfo(discountInfo);

            retVal = KeyValue.pair(discountInfo.getCustomerCardNo(), discountInfoWrapper);
        }
        else {

            // Discount found
            final DiscountInfo discountInfo = new DiscountInfo();
            discountInfo.setCustomerCardNo(invoice.getInvoice().getCustomerNo());
            discountInfo.setEarnedLoyaltyPoints(1d);
            discountInfo.setTotalAmount(currentDiscount.getTotalAmount() + invoice.getInvoice().getTotalAmount());
            discountInfo.setTotalLoyaltyPoints(currentDiscount.getTotalLoyaltyPoints() + 1d);


            // Calculate additional discount points
            final Double additionalPoints =  this.calculateAdditionalPoints(discountInfo.getTotalAmount());
            discountInfo.setEarnedLoyaltyPoints(discountInfo.getEarnedLoyaltyPoints() + additionalPoints);
            discountInfo.setTotalLoyaltyPoints(discountInfo.getTotalLoyaltyPoints() + additionalPoints);

            this.store.put(custNo, discountInfo);

            discountInfoWrapper.setDiscountInfo(discountInfo);

            retVal = KeyValue.pair(discountInfo.getCustomerCardNo(), discountInfoWrapper);

        }

            return retVal;

    }

    @Override
    public void close() {

    }

    public Double calculateAdditionalPoints(final Double amount) {

        final SQLQuery<SpaceDocument> query =
                new SQLQuery<SpaceDocument>(this.spacesStore.getSpaceTypeName(), "");

        final Double totalAmount = sum(this.spacesStore.getSpaceClient(), query, "discount_totalAmount");

        final Double retVal = (amount / (totalAmount + amount) ) * 100;

        return retVal;

    }
}
