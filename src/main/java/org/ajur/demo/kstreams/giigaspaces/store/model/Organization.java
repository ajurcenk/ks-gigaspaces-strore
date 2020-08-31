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

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.metadata.index.SpaceIndexType;

/**
 * This class represents organization object.
 */
public class Organization {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    private Long id;

    private String name;

    private Address addr;

    private OrganizationType type;

    private Timestamp lastUpdated;

    /**
     * Required for binary deserialization.
     */
    public Organization() {
        // No-op.
    }

    /**
     * @param name Organization name.
     */
    public Organization(String name) {
        setId(ID_GEN.incrementAndGet());

        this.setName(name);
        this.setType(OrganizationType.PRIVATE); //default
    }

    /**
     * @param id Organization ID.
     * @param name Organization name.
     */
    public Organization(long id, String name) {
        this.setId(id);
        this.setName(name);
        this.setType(OrganizationType.PRIVATE); //default
    }

    /**
     * @param name Name.
     * @param addr Address.
     * @param type Type.
     * @param lastUpdated Last update time.
     */
    public Organization(String name, Address addr, OrganizationType type, Timestamp lastUpdated) {
        setId(ID_GEN.incrementAndGet());

        this.setName(name);
        this.setAddr(addr);
        this.setType(type);

        this.setLastUpdated(lastUpdated);
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + getId() +
            ", name=" + getName() +
            ", address=" + getAddr() +
            ", type=" + getType() +
            ", lastUpdated=" + getLastUpdated() + ']';
    }

    /** Organization ID (indexed). */
    @SpaceId(autoGenerate = false)
    @SpaceRouting  //This is define explicitly just for clarity. The SpaceID will define routing implicitly if no other
    //SpaceRouting field was defined
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /** Organization name (indexed). */
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /** Address. */
    public Address getAddr() {
        return addr;
    }

    public void setAddr(Address addr) {
        this.addr = addr;
    }

    /** Type. */
    public OrganizationType getType() {
        return type;
    }

    public void setType(OrganizationType type) {
        this.type = type;
    }

    /** Last update time. */
    public Timestamp getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Timestamp lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
