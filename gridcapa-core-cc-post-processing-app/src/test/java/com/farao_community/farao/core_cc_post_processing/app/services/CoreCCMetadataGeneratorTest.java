/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class CoreCCMetadataGeneratorTest {

    private MinioAdapter minioAdapter;
    private CoreCCMetadata coreCCMetadata = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "SUCCESS", "0", "This is an error.", 1);
    private List<CoreCCMetadata> metadataList = List.of(coreCCMetadata);
    private RaoMetadata macroMetadata = new RaoMetadata();
    private boolean fileUploadedToMinio;

    @BeforeEach
    void setUp() {
        fileUploadedToMinio = false;
        minioAdapter = Mockito.mock(MinioAdapter.class);
        setUpMacroMetadata();
    }

    private void setUpMacroMetadata() {
        macroMetadata.setTimeInterval("2023-08-04T11:25:00Z/2023-08-04T12:25:00Z");
        macroMetadata.setRequestReceivedInstant("2023-08-04T11:26:00Z");
        macroMetadata.setRaoRequestFileName("raoRequest.json");
        macroMetadata.setRaoRequestInstant("2023-08-04T11:26:00Z");
        macroMetadata.setStatus("SUCCESS");
        macroMetadata.setOutputsSendingInstant("2023-08-04T11:30:00Z");
        macroMetadata.setComputationStartInstant("2023-08-04T11:27:00Z");
        macroMetadata.setComputationEndInstant("2023-08-04T11:29:00Z");
        macroMetadata.setVersion(1);
    }

    @Test
    void exportMetadataFile() {
        Mockito.doAnswer(answer -> fileUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("/minioFolder/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_20230804-F341-01.csv"), Mockito.any());
        CoreCCMetadataGenerator coreCCMetadataGenerator = new CoreCCMetadataGenerator(minioAdapter);
        coreCCMetadataGenerator.exportMetadataFile("/minioFolder", metadataList, macroMetadata);
        assertTrue(fileUploadedToMinio);
    }

    @Test
    void generateMetadataFileName() {
        assertEquals("CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_20230804-F341-01.csv", CoreCCMetadataGenerator.generateMetadataFileName("2023-08-04T11:06:00Z", 1));
    }

    @Test
    void generateOutputsDestinationPath() {
        assertEquals("path/outputs/file.txt", CoreCCMetadataGenerator.generateOutputsDestinationPath("path", "file.txt"));
    }
}
