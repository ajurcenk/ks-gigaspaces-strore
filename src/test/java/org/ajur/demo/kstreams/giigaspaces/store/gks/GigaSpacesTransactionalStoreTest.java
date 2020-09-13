package org.ajur.demo.kstreams.giigaspaces.store.gks;

import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import org.ajur.demo.kstreams.giigaspaces.store.model.Customer;
import org.ajur.demo.kstreams.giigaspaces.store.model.DiscountInfo;
import org.ajur.demo.kstreams.giigaspaces.store.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.core.transaction.manager.DistributedJiniTxManagerConfigurer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GigaSpacesTransactionalStoreTest {

    public static final String SPACE_DOC_TYPE = "test_store_doc_v3";

    protected GigaSpace client;
    protected UrlSpaceConfigurer configurer;
    protected PlatformTransactionManager ptm;
    protected Serde<DiscountInfo> valueSerde;
    protected DiscountGigaSpacePropertiesExtractor propertiesExtractor = new DiscountGigaSpacePropertiesExtractor();

    @Before
    public void setUp() throws Exception {

        // Create transaction manager
        ptm = new DistributedJiniTxManagerConfigurer().transactionManager();
        configurer = new UrlSpaceConfigurer("jini://*/*/orders-joiner" );
        client = new GigaSpaceConfigurer(configurer).transactionManager(ptm).clustered(true).gigaSpace();

        // Create space type
        final SpaceTypeDescriptorBuilder docBuilder = new SpaceTypeDescriptorBuilder(SPACE_DOC_TYPE);
        propertiesExtractor.getSpaceProperties()
                .entrySet()
                .stream()
                .forEach(spaceProp -> {
                    docBuilder.addFixedProperty(spaceProp.getValue().getName(), spaceProp.getValue().getType());

                    // Add index
                    if (spaceProp.getValue().isIndexed()) {
                        docBuilder.addPropertyIndex(spaceProp.getValue().getName(), SpaceIndexType.EQUAL);
                    }
                });


        // Create test space document
        final SpaceTypeDescriptor typeDescriptor = docBuilder.idProperty("key_string", false)
                .addFixedProperty("key", byte[].class)
                .addFixedProperty("key_string", String.class)
                .addFixedProperty("value", byte[].class)
                .create();

        client.getTypeManager().registerTypeDescriptor(typeDescriptor);

        // Serdes
        valueSerde = SerdesFactory.from(DiscountInfo.class);


    }

    @After
    public void clean() {

        configurer.close();
    }

    @Test
    public void testInsert() {

        final DiscountInfo discountInfo = new DiscountInfo();
        discountInfo.setCustomerCardNo("1");
        discountInfo.setTotalAmount(100.00);

        SpaceDocument spaceDocument = createSpaceDoc("1", discountInfo);
        client.write(spaceDocument);
    }


    @Test
    public void testReadById() {

        final String key = "1";
        final String id = convertToStringKey(key);

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, id));

        assertNotNull(spaceDoc);
    }


    @Test
    public void testUpdate() {

        final String key = "1";
        final String id = convertToStringKey(key);

        final SpaceDocument spaceDoc = client.readById(new IdQuery<SpaceDocument>(SPACE_DOC_TYPE, id));

        assertNotNull(spaceDoc);

        // Update existing doc
        spaceDoc.setProperty("totalAmount", 120.00d);

        client.write(spaceDoc);
    }


    private SpaceDocument createSpaceDoc(final String key, DiscountInfo discountInfo) {

        final Map<String, Object> objectProps = new HashMap<>();

        final byte[] bytesKey = Serdes.String().serializer().serialize("",key);
        final String encodedKey = Base64.getEncoder().encodeToString(bytesKey);

        objectProps.put("key", bytesKey);
        objectProps.put("key_string", encodedKey);
        objectProps.put("value", this.valueSerde.serializer().serialize("", discountInfo));
        objectProps.putAll(this.propertiesExtractor.getSpaceValues(discountInfo));

        final SpaceDocument spaceDoc = new SpaceDocument(SPACE_DOC_TYPE,objectProps);

        return  spaceDoc;
    }


    private String convertToStringKey(final String id) {

        final byte[] bytesKey = Serdes.String().serializer().serialize("",id);
        final String encodedKey = Base64.getEncoder().encodeToString(bytesKey);

        return  encodedKey;
    }


}
