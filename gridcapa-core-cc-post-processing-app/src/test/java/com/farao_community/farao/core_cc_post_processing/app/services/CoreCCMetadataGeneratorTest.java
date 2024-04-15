/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
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
    private final RaoMetadata successMacroMetadata = new RaoMetadata();
    private final RaoMetadata errorMacroMetadata = new RaoMetadata();

    private void setUpSuccessMacroMetadata() {
        successMacroMetadata.setTimeInterval("2023-08-04T11:25:00Z/2023-08-04T12:25:00Z");
        successMacroMetadata.setRequestReceivedInstant("2023-08-04T11:26:00Z");
        successMacroMetadata.setRaoRequestFileName("raoRequest.json");
        successMacroMetadata.setRaoRequestInstant("2023-08-04T11:26:00Z");
        successMacroMetadata.setStatus("SUCCESS");
        successMacroMetadata.setOutputsSendingInstant("2023-08-04T11:30:00Z");
        successMacroMetadata.setComputationStartInstant("2023-08-04T11:27:00Z");
        successMacroMetadata.setComputationEndInstant("2023-08-04T11:29:00Z");
        successMacroMetadata.setVersion(1);
        successMacroMetadata.setCorrelationId("6fe0a389-9315-417e-956d-b3fbaa479caz");
    }

    private void setUpErrorMacroMetadata() {
        errorMacroMetadata.setTimeInterval("2023-08-04T11:25:00Z/2023-08-04T12:25:00Z");
        errorMacroMetadata.setRequestReceivedInstant("2023-08-04T11:26:00Z");
        errorMacroMetadata.setRaoRequestFileName("raoRequest.json");
        errorMacroMetadata.setRaoRequestInstant("2023-08-04T11:26:00Z");
        errorMacroMetadata.setStatus("ERROR");
        errorMacroMetadata.setOutputsSendingInstant("2023-08-04T11:30:00Z");
        errorMacroMetadata.setComputationStartInstant("2023-08-04T11:27:00Z");
        errorMacroMetadata.setComputationEndInstant("2023-08-04T11:29:00Z");
        errorMacroMetadata.setVersion(1);
        errorMacroMetadata.setCorrelationId("6fe0a389-9315-417e-956d-b3fbaa479caz");
    }

    @Test
    void successfullyGeneratedMetadataCsv() throws IOException {
        setUpSuccessMacroMetadata();
        final String result = CoreCCMetadataGenerator.generateMetadataCsv(metadataList, successMacroMetadata);
        assertTrue(Utils.isFileContentEqualToString(result, "/services/metadataSuccess.csv"));
    }

    @Test
    void exportErrorMetadataFile() throws IOException {
        setUpErrorMacroMetadata();
        final String result = CoreCCMetadataGenerator.generateMetadataCsv(metadataList, errorMacroMetadata);
        assertTrue(Utils.isFileContentEqualToString(result, "/services/metadataError.csv"));
    }
}
