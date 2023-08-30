/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.core_cc_post_processing.app.services.RaoIXmlResponseGenerator;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.*;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class PostProcessingServiceTest {

    private final MinioAdapter minioAdapter = Mockito.mock(MinioAdapter.class);
    private final LocalDate localDate = LocalDate.of(2023, 8, 4);
    private final Set<TaskDto> tasksToPostProcess = Set.of(Utils.SUCCESS_TASK);
    private List<byte[]> logList = new ArrayList<>();
    private final InputStream inputMetadataInputStream = getClass().getResourceAsStream("/services/metadatas/coreCCMetadata.json");
    private final ProcessFileDto metadataProcessFile = new ProcessFileDto("/CORE/CC/coreCCMetadata.json", "METADATA", ProcessFileStatus.VALIDATED, "coreCCMetadata.json", OffsetDateTime.parse("2019-01-08T12:30Z"));
    private final TaskDto task = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000001"), OffsetDateTime.parse("2019-01-08T12:30Z"), TaskStatus.SUCCESS, List.of(metadataProcessFile), List.of(), List.of());

    @BeforeEach
    void setUp() {
        Mockito.doReturn(inputMetadataInputStream).when(minioAdapter).getFile("coreCCMetadata.json");
    }

    private void initLogs() {
        String log1 = "This is the first log.";
        String log2 = "Here is another one.";
        String log3 = "Hello World!";
        logList = List.of(log1.getBytes(), log2.getBytes(), log3.getBytes());
    }

    private PostProcessingService initPostProcessingService(MinioAdapter adapter) {
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(adapter);
        DailyF303Generator dailyF303Generator = Mockito.mock(DailyF303Generator.class);
        FlowBasedConstraintDocument dailyFlowBasedConstraintDocument = Mockito.mock(FlowBasedConstraintDocument.class);
        Mockito.doReturn(dailyFlowBasedConstraintDocument).when(dailyF303Generator).generate(Mockito.any());
        return new PostProcessingService(adapter, raoIXmlResponseGenerator, dailyF303Generator);
    }

    @Test
    void processTasks() throws IOException {
        initLogs();
        PostProcessingService postProcessingService = initPostProcessingService(Utils.MINIO_FILE_WRITER);
        postProcessingService.processTasks(localDate, tasksToPostProcess, logList);
        String outputDir = "/tmp/outputs/RAO_OUTPUTS_DIR/2023-08-04/outputs/";
        assertAllOutputsGenerated(outputDir);
        Utils.assertFilesContentEqual("/services/export/F303.xml", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F303_20230804-F303-01.xml", true);
        Utils.assertFilesContentEqual("/services/export/F305.xml", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml", true);
        Utils.assertFilesContentEqual("/services/export/F341.csv", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_20230804-F341-01.csv", true);
        // Zips not tested because of .gitignore
        FileUtils.deleteDirectory(new File("/tmp/outputs/"));
    }

    private static void assertAllOutputsGenerated(String outputDir) {
        assertTrue(new File(outputDir).exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F299_20230804-F299-01.zip").exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F303_20230804-F303-01.xml").exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F304_20230804-F304-01.zip").exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml").exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_20230804-F341-01.csv").exists());
        assertTrue(new File(outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F342_20230804-F342-01.zip").exists());
    }

    // Tests on fetchMetadataFromMinio

    private void assertExceptionWithIncoherentMetadata(String differentMetadataFile, String expectedErrorMessage) {
        InputStream inputWrongMetadataInputStream = getClass().getResourceAsStream("/services/metadatas/" + differentMetadataFile);
        Mockito.doReturn(inputWrongMetadataInputStream).when(minioAdapter).getFile(differentMetadataFile);
        PostProcessingService postProcessingService = initPostProcessingService(minioAdapter);
        ProcessFileDto wrongMetadataProcessFile = new ProcessFileDto("/CORE/CC/" + differentMetadataFile, "METADATA", ProcessFileStatus.VALIDATED, differentMetadataFile, OffsetDateTime.parse("2019-01-08T12:30Z"));
        TaskDto taskWrongMetadata = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000002"), OffsetDateTime.parse("2019-01-08T13:30Z"), TaskStatus.SUCCESS, List.of(wrongMetadataProcessFile), List.of(), List.of());
        Map<TaskDto, ProcessFileDto> metadatas = Map.of(task, metadataProcessFile, taskWrongMetadata, wrongMetadataProcessFile);
        CoreCCPostProcessingInternalException exception = assertThrows(CoreCCPostProcessingInternalException.class, () -> postProcessingService.fetchMetadataFromMinio(metadatas));
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @Test
    void fetchMetadataFromMinioSeveralTimeIntervals() {
        assertExceptionWithIncoherentMetadata("coreCCMetadataDifferentTimeInterval.json", "Wrong time Interval in metadata");
    }

    @Test
    void fetchMetadataFromMinioSeveralRaoRequests() {
        assertExceptionWithIncoherentMetadata("coreCCMetadataDifferentRaoRequest.json", "Wrong Rao request file name in metadata");
    }

    @Test
    void fetchMetadataFromMinioSeveralVersions() {
        assertExceptionWithIncoherentMetadata("coreCCMetadataDifferentVersion.json", "Wrong version in metadata");
    }

    @Test
    void fetchMetadataFromMinioSeveralCorrelationIds() {
        assertExceptionWithIncoherentMetadata("coreCCMetadataDifferentCorrelationId.json", "Wrong correlationId in metadata");
    }

    @Test
    void fetchMetadataFromMinioWithError() {
        assertExceptionWithIncoherentMetadata("coreCCMetadataError.json", "Invalid overall status");
    }

    @Test
    void fetchMetadataFromMinio() {
        PostProcessingService postProcessingService = initPostProcessingService(minioAdapter);
        // Process file with NOT_CREATED floaf -> will be filtered out
        ProcessFileDto runningMetadataProcessFile = new ProcessFileDto("/CORE/CC/coreCCMetadataRunning.json", "METADATA", ProcessFileStatus.NOT_PRESENT, "coreCCMetadataRunning.json", OffsetDateTime.parse("2019-01-08T12:30Z"));
        TaskDto taskRunning = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000002"), OffsetDateTime.parse("2019-01-08T13:30Z"), TaskStatus.RUNNING, List.of(runningMetadataProcessFile), List.of(), List.of());
        Map<TaskDto, ProcessFileDto> metadatas = Map.of(task, metadataProcessFile, taskRunning, runningMetadataProcessFile);

        Map<UUID, CoreCCMetadata> metadataMap = postProcessingService.fetchMetadataFromMinio(metadatas);
        assertEquals(1, metadataMap.size());

        CoreCCMetadata metadata = metadataMap.get(UUID.fromString("00000000-0000-0000-0000-000000000001"));
        assertEquals("raoRequest.json", metadata.getRaoRequestFileName());
        assertEquals("2019-01-08T12:30Z", metadata.getRequestReceivedInstant());
        assertEquals("2019-01-08T12:30Z", metadata.getRaoRequestInstant());
        assertEquals("2019-01-08T12:30Z", metadata.getComputationStart());
        assertEquals("2019-01-08T12:31Z", metadata.getComputationEnd());
        assertEquals("2019-01-07T23:00Z/2019-01-08T23:00Z", metadata.getTimeInterval());
        assertEquals("00000000-0000-0000-0000-000000000000", metadata.getCorrelationId());
        assertEquals("SUCCESS", metadata.getStatus());
        assertEquals("0", metadata.getErrorCode());
        assertEquals("This is an error.", metadata.getErrorMessage());
        assertEquals(1, metadata.getVersion());
    }
}
