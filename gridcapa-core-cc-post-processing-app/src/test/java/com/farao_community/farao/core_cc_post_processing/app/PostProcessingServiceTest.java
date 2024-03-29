/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.craccreation.creator.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.TEMP_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private void initLogs() throws IOException {
        logList = List.of(fileToByteArray("logs.zip"));
    }

    private PostProcessingService initPostProcessingService(MinioAdapter adapter) {
        DailyF303Generator dailyF303Generator = Mockito.mock(DailyF303Generator.class);
        FlowBasedConstraintDocument dailyFlowBasedConstraintDocument = Mockito.mock(FlowBasedConstraintDocument.class);
        Mockito.doReturn(dailyFlowBasedConstraintDocument)
                .when(dailyF303Generator)
                .generate(Mockito.anyMap(), Mockito.anyMap());
        return new PostProcessingService(adapter);
    }

    @Test
    void processTasks() throws IOException {
        initLogs();
        PostProcessingService postProcessingService = initPostProcessingService(Utils.MINIO_FILE_WRITER);
        postProcessingService.processTasks(localDate, tasksToPostProcess, logList);
        String outputDir = TEMP_DIR + "/outputs/RAO_OUTPUTS_DIR/2023-08-04/outputs/";
        assertAllOutputsGenerated(outputDir);
        Utils.assertFilesContentEqual("/services/export/F303.xml", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F303_20230804-F303-01.xml", true);
        Utils.assertFilesContentEqual("/services/export/F305.xml", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml", true);
        Utils.assertFilesContentEqual("/services/export/F341.csv", outputDir + "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_20230804-F341-01.csv", true);
        // Test zips content
        unzipAndAssertF299Content();
        unzipAndAssertF304Content();
        unzipAndAssertF342Content();
        // Delete /tmp/outputs test folder
        FileUtils.deleteDirectory(new File(TEMP_DIR + "/outputs/"));
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

    private static List<File> extractZip(int fileId) throws IOException {
        String outputDir = TEMP_DIR + "/outputs/RAO_OUTPUTS_DIR/2023-08-04/outputs/";
        String archiveName = "CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F" + fileId + "_20230804-F" + fileId + "-01.zip";
        ZipFile zipFile = new ZipFile(outputDir + archiveName);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        List<File> unzippedFiles = new ArrayList<>();
        int numberOfFiles = 0;
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            InputStream stream = zipFile.getInputStream(entry);
            String tmpFile = TEMP_DIR + "/outputs/tempFile-F" + fileId + "-" + numberOfFiles;
            File targetFile = new File(tmpFile);
            OutputStream outStream = new FileOutputStream(targetFile);
            IOUtils.copy(stream, outStream);
            unzippedFiles.add(targetFile);
            numberOfFiles++;
        }
        return unzippedFiles;
    }

    private static void unzipAndAssertF299Content() throws IOException {
        try {
            List<File> unzippedFiles = extractZip(299);
            assertEquals(1, unzippedFiles.size());
            Utils.assertFilesContentEqual("/services/export/F299-cne.xml", unzippedFiles.get(0).getAbsolutePath());
        } catch (IOException e) {
            throw new IOException("File not found.");
        }
    }

    private static void unzipAndAssertF304Content() throws IOException {
        try {
            List<File> unzippedFiles = extractZip(304);
            assertEquals(2, unzippedFiles.size());
            Utils.assertFilesContentEqual("/services/export/F304-CGM_XML_Header.xml", unzippedFiles.get(0).getAbsolutePath(), true);
            Utils.assertFilesContentEqual("/services/export/F304-network.uct", unzippedFiles.get(1).getAbsolutePath());
        } catch (IOException e) {
            throw new IOException("File not found.");
        }
    }

    private static void unzipAndAssertF342Content() throws IOException {
        try {
            List<File> unzippedFiles = extractZip(342);
            assertEquals(2, unzippedFiles.size());
            Utils.assertFilesContentEqual("/services/export/logs1.txt", unzippedFiles.get(0).getAbsolutePath());
            Utils.assertFilesContentEqual("/services/export/logs2.txt", unzippedFiles.get(1).getAbsolutePath());
        } catch (IOException e) {
            throw new IOException("File not found.", e);
        }
    }

    private byte[] fileToByteArray(String filename) throws IOException {
        String filePath = "/services/" + filename;
        return getClass().getResourceAsStream(filePath).readAllBytes();
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
