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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.metadata.index.SpaceIndexType;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.openspaces.textsearch.SpaceTextAnalyzer;
import org.openspaces.textsearch.SpaceTextIndex;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    private Long id;

    private Long orgId;

    private String firstName;

    private String lastName;

    private String resume;

    private double salary;


    /**
     * Default constructor.
     */
    public Person() {
        // No-op.
    }

    /**
     * Constructs person record.
     *
     * @param org       Organization.
     * @param firstName First name.
     * @param lastName  Last name.
     * @param salary    Salary.
     * @param resume    Resume text.
     */
    public Person(Organization org, String firstName, String lastName, double salary, String resume) {
        // Generate unique ID for this person.
        setId(ID_GEN.incrementAndGet());

        setOrgId(org.getId());

        this.setFirstName(firstName);
        this.setLastName(lastName);
        this.setSalary(salary);
        this.setResume(resume);
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param orgId Organization ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary    Salary.
     * @param resume    Resume text.
     */
    public Person(Long id, Long orgId, String firstName, String lastName, double salary, String resume) {
        this.setId(id);
        this.setOrgId(orgId);
        this.setFirstName(firstName);
        this.setLastName(lastName);
        this.setSalary(salary);
        this.setResume(resume);
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param firstName First name.
     * @param lastName Last name.
     */
    public Person(Long id, String firstName, String lastName) {
        this.setId(id);

        this.setFirstName(firstName);
        this.setLastName(lastName);
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return "Person [id=" + getId() +
                ", orgId=" + getOrgId() +
                ", lastName=" + getLastName() +
                ", firstName=" + getFirstName() +
                ", salary=" + getSalary() +
                ", resume=" + getResume() + ']';
    }

    /** Person ID (indexed). */
    @SpaceId(autoGenerate = false)
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /** Organization ID (indexed). */
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    @SpaceRouting
    public Long getOrgId() {
        return orgId;
    }

    public void setOrgId(Long orgId) {
        this.orgId = orgId;
    }

    /** First name (not-indexed). */
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    /** Last name (not indexed). */
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    /** Resume text (create LUCENE-based TEXT index for this field). */
    @SpaceTextIndex
    @SpaceTextAnalyzer(analyzer = StandardAnalyzer.class)
    public String getResume() {
        return resume;
    }

    public void setResume(String resume) {
        this.resume = resume;
    }

    /** Salary (indexed). */
    @SpaceIndex(type = SpaceIndexType.EQUAL_AND_ORDERED)
    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }
}
