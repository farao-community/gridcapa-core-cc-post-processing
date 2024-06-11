/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Amira Kahya {@literal <amira.kahya at rte-france.com>}
 */
class NamingRulesTest {

    private final LocalDate testDate = LocalDate.of(2023, 07, 31);

    @Test
    void generateMetadataFileNameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230727-F341-01.csv", NamingRules.generateMetadataFileName("2023-07-27T14:02:00Z", 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230723-F341-02.csv", NamingRules.generateMetadataFileName("2023-07-23T14:02:00Z", 2));
        assertNotEquals("22XCORESO------S_1011001C--00236Y_CORE-FB-341_20230723-F341-02.csv", NamingRules.generateMetadataFileName("2023-07-23T14:02:00Z", 2));
    }

    @Test
    void generateRF305FileNameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230731-F305-01.xml", NamingRules.generateRF305FileName(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230731-F305-99.xml", NamingRules.generateRF305FileName(testDate, 99));
        assertNotEquals("22XCORESO------S_1011001C--00236Y_CORE-FB-305_20230731-F305-01.xml", NamingRules.generateRF305FileName(testDate, 1));
    }

    @Test
    void generateOptimizedCbFileNameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-01.xml", NamingRules.generateOptimizedCbFileName(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-98.xml", NamingRules.generateOptimizedCbFileName(testDate, 98));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-01.xml", NamingRules.generateOptimizedCbFileName(testDate, 2));
    }

    @Test
    void generateCgmZipNameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-01.zip", NamingRules.generateCgmZipName(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-22.zip", NamingRules.generateCgmZipName(testDate, 22));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-01.zip", NamingRules.generateCgmZipName(testDate, 2));
    }

    @Test
    void generateCneZipNameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-01.zip", NamingRules.generateCneZipName(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-66.zip", NamingRules.generateCneZipName(testDate, 66));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-01.zip", NamingRules.generateCneZipName(testDate, 2));
    }

    @Test
    void generateRaoResultFilenameTest() {
        assertEquals("CASTOR-INTERNAL-RESULTS_20230731.zip", NamingRules.generateRaoResultFilename(testDate));
    }
}
