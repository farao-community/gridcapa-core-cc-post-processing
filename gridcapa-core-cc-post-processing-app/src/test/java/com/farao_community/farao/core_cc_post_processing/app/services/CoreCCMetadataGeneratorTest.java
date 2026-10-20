/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.core_cc_post_processing.app.entities.DailyMetadata;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
@SpringBootTest
class CoreCCMetadataGeneratorTest {

    private final List<CoreCCMetadata> metadataList = List.of(Utils.CORE_CC_METADATA_SUCCESS);
    private final DailyMetadata successDailyMetadata = new DailyMetadata();
    private final DailyMetadata errorDailyMetadata = new DailyMetadata();

    private void setUpSuccessDailyMetadata() {
        successDailyMetadata.setTimeInterval("2023-08-04T11:25:00Z/2023-08-04T12:25:00Z");
        successDailyMetadata.setRequestReceivedInstant("2023-08-04T11:26:00Z");
        successDailyMetadata.setRaoRequestFileName("raoRequest.json");
        successDailyMetadata.setStatus("SUCCESS");
        successDailyMetadata.setOutputsSendingInstant("2023-08-04T11:30:00Z");
        successDailyMetadata.setContinentalComputationStart("2023-08-04T11:27:00Z");
        successDailyMetadata.setContinentalComputationEnd("2023-08-04T11:29:00Z");
    }

    private void setUpErrorDailyMetadata() {
        errorDailyMetadata.setTimeInterval("2023-08-04T11:25:00Z/2023-08-04T12:25:00Z");
        errorDailyMetadata.setRequestReceivedInstant("2023-08-04T11:26:00Z");
        errorDailyMetadata.setRaoRequestFileName("raoRequest.json");
        errorDailyMetadata.setStatus("ERROR");
        errorDailyMetadata.setOutputsSendingInstant("2023-08-04T11:30:00Z");
        errorDailyMetadata.setContinentalComputationStart("2023-08-04T11:27:00Z");
        errorDailyMetadata.setContinentalComputationEnd("2023-08-04T11:29:00Z");
    }

    @Test
    void successfullyGeneratedMetadataCsv() throws IOException {
        setUpSuccessDailyMetadata();
        final String result = CoreCCMetadataGenerator.generateMetadataCsv(metadataList, successDailyMetadata);
        assertTrue(Utils.isFileContentEqualToString(result, "/services/metadataSuccess.csv"));
    }

    @Test
    void exportErrorMetadataFile() throws IOException {
        setUpErrorDailyMetadata();
        final String result = CoreCCMetadataGenerator.generateMetadataCsv(metadataList, errorDailyMetadata);
        assertTrue(Utils.isFileContentEqualToString(result, "/services/metadataError.csv"));
    }
}
