/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ajur.demo.kstreams.giigaspaces.store.model;



/**
 * Employee address.
 */
public class Address  {
    /** Street. */
    private String street;

    /** ZIP code. */
    private int zip;

    /**
     * Required for binary deserialization.
     */
    public Address() {
        // No-op.
    }

    /**
     * @param street Street.
     * @param zip ZIP code.
     */
    public Address(String street, int zip) {
        this.street = street;
        this.zip = zip;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Address [street=" + street +
            ", zip=" + zip + ']';
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public int getZip() {
        return zip;
    }

    public void setZip(int zip) {
        this.zip = zip;
    }

}
