package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class Utils {

    public static void changeSerde(AbstractStream ksImp, Serde serde) throws NoSuchFieldException, IllegalAccessException {

        Field field = AbstractStream.class.getDeclaredField( "valSerde" );
        field.setAccessible( true );
        field.set( ksImp, serde );

    }
}