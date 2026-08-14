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
    private final String testInstant = "2023-07-31T00:00:00.000Z";

    @Test
    void generateOutputsDestinationPathTest() {
        assertEquals("prefix/outputs/filename", NamingRules.generateOutputsDestinationPath("prefix", "filename"));
    }

    // ZIP

    @Test
    void generateCgmZipFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-01.zip", NamingRules.generateCgmZipFilename(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-22.zip", NamingRules.generateCgmZipFilename(testDate, 22));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-304_20230731-F304-01.zip", NamingRules.generateCgmZipFilename(testDate, 2));
    }

    @Test
    void generateCneZipFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-01.zip", NamingRules.generateCneZipFilename(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-66.zip", NamingRules.generateCneZipFilename(testDate, 66));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_20230731-F299-01.zip", NamingRules.generateCneZipFilename(testDate, 2));
    }

    @Test
    void generateLogsZipFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-342_20230731-F342-01.zip", NamingRules.generateLogsZipFilename(testInstant, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-342_20230731-F342-66.zip", NamingRules.generateLogsZipFilename(testInstant, 66));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-342_20230731-F342-01.zip", NamingRules.generateLogsZipFilename(testInstant, 2));
    }

    @Test
    void generateRaoResultZipFilenameTest() {
        assertEquals("CASTOR-INTERNAL-RESULTS_20230731.zip", NamingRules.generateRaoResultZipFilename(testDate));
    }

    // NON-ZIP

    @Test
    void generateCbcoraFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-01.xml", NamingRules.generateCbcoraFilename(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-98.xml", NamingRules.generateCbcoraFilename(testDate, 98));
        assertNotEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230731-F303-01.xml", NamingRules.generateCbcoraFilename(testDate, 2));
    }

    @Test
    void generateCgmFilenameTest() {
        assertEquals("20230727_1630_2D4_UX1.uct", NamingRules.generateCgmFilename("2023-07-27T14:02:00Z", 1));
        // 25-timestamp case
        assertEquals("20261025_0230_2D7_UX2.uct", NamingRules.generateCgmFilename("2026-10-25T00:02:00Z", 2));
        assertEquals("20261025_B230_2D7_UX3.uct", NamingRules.generateCgmFilename("2026-10-25T01:02:00Z", 3));
        assertEquals("20261025_0330_2D7_UX3.uct", NamingRules.generateCgmFilename("2026-10-25T02:02:00Z", 3));
    }

    @Test
    void generateMetadataFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230727-F341-01.csv", NamingRules.generateMetadataFilename("2023-07-27T14:02:00Z", 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230723-F341-02.csv", NamingRules.generateMetadataFilename("2023-07-23T14:02:00Z", 2));
        assertNotEquals("22XCORESO------S_1011001C--00236Y_CORE-FB-341_20230723-F341-02.csv", NamingRules.generateMetadataFilename("2023-07-23T14:02:00Z", 2));
    }

    @Test
    void generateRaoResponseFilenameTest() {
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230731-F305-01.xml", NamingRules.generateRaoResponseFilename(testDate, 1));
        assertEquals("22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230731-F305-99.xml", NamingRules.generateRaoResponseFilename(testDate, 99));
        assertNotEquals("22XCORESO------S_1011001C--00236Y_CORE-FB-305_20230731-F305-01.xml", NamingRules.generateRaoResponseFilename(testDate, 1));
    }
}
