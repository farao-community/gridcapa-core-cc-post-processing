/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.HeaderType;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.PayloadType;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseItem;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.xml.datatype.DatatypeConfigurationException;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
@SpringBootTest
class RaoIXmlResponseGeneratorTest {

    private final LocalDate localDate = LocalDate.of(2023, 8, 4);
    private final String startInstantString = "2023-08-04T14:46:00Z";
    private final OffsetDateTime startInstant = OffsetDateTime.parse(startInstantString);
    private final String endInstantString = "2023-08-04T14:47:00Z";
    private final OffsetDateTime endInstant = OffsetDateTime.parse(endInstantString);
    private final String correlationId = "correlationId";
    private Set<TaskDto> taskDtos;
    private final String cgmsArchiveTempPath = "/tmp/gridcapa/cgms";
    private final String xmlHeaderFileName = "/services/CGM_XML_Header.xml";
    private final Path xmlHeaderFilePath = Paths.get(Objects.requireNonNull(getClass().getResource(xmlHeaderFileName)).getPath());
    private final File xmlHeaderFile = new File(xmlHeaderFilePath.toString());
    private boolean raoResponseIsUploadedToMinio;
    private final CoreCCMetadata coreCCMetadataErrorTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "ERROR", "1", "This is an error.", 1);
    private final CoreCCMetadata coreCCMetadataRunningTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "RUNNING", "0", "This is an error.", 1);
    private final CoreCCMetadata coreCCMetadataSuccessTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "SUCCESS", "0", "This is an error.", 1);
    private final Map<UUID, CoreCCMetadata> metadataMap = new HashMap<>();

    @Autowired
    RaoIXmlResponseGenerator raoIXmlResponseGenerator;

    @Autowired
    MinioAdapter minioAdapter;

    @BeforeEach
    void setUp() {
        minioAdapter = Mockito.mock(MinioAdapter.class);
        raoResponseIsUploadedToMinio = false;
    }

    @Test
    void generateCgmXmlHeaderFile() throws IOException {
        initTasksForCgmXmlHeader();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        File generatedXmlHeaderFile = new File(cgmsArchiveTempPath, "CGM_XML_Header.xml");
        // First pass with not existing directory
        compareOriginalAndGeneratedXmlHeaders(raoIXmlResponseGenerator, generatedXmlHeaderFile);
        // Second pass with already existing directory
        compareOriginalAndGeneratedXmlHeaders(raoIXmlResponseGenerator, generatedXmlHeaderFile);
        // Delete the temporary directory
        FileUtils.deleteDirectory(new File(generatedXmlHeaderFile.getParent()));
        assertFalse(Files.exists(generatedXmlHeaderFile.getParentFile().toPath()));
    }

    private void initTasksForCgmXmlHeader() {
        TaskDto taskDtoStart = Mockito.mock(TaskDto.class);
        Mockito.doReturn(startInstant).when(taskDtoStart).getTimestamp();
        Mockito.doReturn(TaskStatus.SUCCESS).when(taskDtoStart).getStatus();
        TaskDto taskDtoEnd = Mockito.mock(TaskDto.class);
        Mockito.doReturn(endInstant).when(taskDtoEnd).getTimestamp();
        Mockito.doReturn(TaskStatus.SUCCESS).when(taskDtoEnd).getStatus();
        taskDtos = Set.of(taskDtoStart, taskDtoEnd);
    }

    private void changeDateInGeneratedCggmXmlHeader(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String pattern = "<Timestamp>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z</Timestamp>";
        String replaceBy = "<Timestamp>2023-08-04T13:03:23.900189Z</Timestamp>";
        String line;
        StringBuilder oldText = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            oldText.append(line).append("\r\n");
        }
        reader.close();
        String newText = oldText.toString().replaceAll(pattern, replaceBy);
        FileWriter writer = new FileWriter(file.getAbsolutePath());
        writer.write(newText);
        writer.close();
    }

    private void compareOriginalAndGeneratedXmlHeaders(RaoIXmlResponseGenerator raoIXmlResponseGenerator, File generatedXmlHeaderFile) throws IOException {
        raoIXmlResponseGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId);
        // File's generation date is automatically added and need to be changed to match the template's
        changeDateInGeneratedCggmXmlHeader(generatedXmlHeaderFile);
        assertTrue(Files.exists(generatedXmlHeaderFile.toPath()));
        assertThat(xmlHeaderFile).hasSameTextualContentAs(generatedXmlHeaderFile);
    }

    @Test
    void generateRaoResponse() {
        initTasksForRaoResponse();
        initMetadataMap();
        Mockito.doAnswer(answer -> raoResponseIsUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("/minioFolder/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml"), Mockito.any());
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        String targetMinioFolder = "/minioFolder";
        raoIXmlResponseGenerator.generateRaoResponse(taskDtos, targetMinioFolder, localDate, correlationId, metadataMap);
        assertTrue(raoResponseIsUploadedToMinio);
    }

    private void initTasksForRaoResponse() {
        taskDtos = Set.of(Utils.ERROR_TASK, Utils.RUNNING_TASK, Utils.SUCCESS_TASK);
    }

    private void initMetadataMap() {
        metadataMap.put(Utils.ERROR_TASK.getId(), coreCCMetadataErrorTask);
        metadataMap.put(Utils.RUNNING_TASK.getId(), coreCCMetadataRunningTask);
        metadataMap.put(Utils.SUCCESS_TASK.getId(), coreCCMetadataSuccessTask);
    }

    @Test
    void generateRaoResponseHeader() throws DatatypeConfigurationException {
        ResponseMessageType responseMessage = new ResponseMessageType();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        raoIXmlResponseGenerator.generateRaoResponseHeader(responseMessage, localDate, correlationId);
        HeaderType header = responseMessage.getHeader();
        assertEquals("created", header.getVerb());
        assertEquals("OptimizedRemedialActions", header.getNoun());
        assertEquals("1", header.getRevision());
        assertEquals("PRODUCTION", header.getContext());
        assertEquals("22XCORESO------S", header.getSource());
        assertEquals("17XTSO-CS------W", header.getReplyAddress());
        assertEquals("22XCORESO------S-20230804-F305", header.getMessageID());
        assertEquals("correlationId", header.getCorrelationID());
    }

    @Test
    void generateRaoResponsePayLoad() {
        ResponseMessageType responseMessage = new ResponseMessageType();
        initTasksForRaoResponse();
        initMetadataMap();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        raoIXmlResponseGenerator.generateRaoResponsePayLoad(taskDtos, responseMessage, localDate, metadataMap);
        PayloadType payload = responseMessage.getPayload();

        assertEquals(3, payload.getResponseItems().getResponseItem().size());

        ResponseItem successResponseItem = payload.getResponseItems().getResponseItem().get(0);
        assertEquals("2023-08-21T15:16Z/2023-08-21T16:16Z", successResponseItem.getTimeInterval());
        assertEquals(3, successResponseItem.getFiles().getFile().size());
        assertEquals("OPTIMIZED_CGM", successResponseItem.getFiles().getFile().get(0).getCode());
        assertEquals("documentIdentification://22XCORESO------S-20230804-F304v1", successResponseItem.getFiles().getFile().get(0).getUrl());
        assertEquals("OPTIMIZED_CB", successResponseItem.getFiles().getFile().get(1).getCode());
        assertEquals("documentIdentification://22XCORESO------S-20230804-F303v1", successResponseItem.getFiles().getFile().get(1).getUrl());
        assertEquals("RAO_REPORT", successResponseItem.getFiles().getFile().get(2).getCode());
        assertEquals("documentIdentification://CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F342_20230804-F342-0V.zip", successResponseItem.getFiles().getFile().get(2).getUrl());

        ResponseItem errorResponseItem = payload.getResponseItems().getResponseItem().get(1);
        assertEquals("2023-08-21T15:16Z/2023-08-21T16:16Z", errorResponseItem.getTimeInterval());
        assertEquals("1", errorResponseItem.getError().getCode());
        assertEquals("FATAL", errorResponseItem.getError().getLevel());
        assertNull(errorResponseItem.getFiles());

        ResponseItem runningrResponseItem = payload.getResponseItems().getResponseItem().get(2);
        assertEquals("2023-08-21T15:16Z/2023-08-21T16:16Z", runningrResponseItem.getTimeInterval());
        assertEquals("INFORM", runningrResponseItem.getError().getLevel());
        assertNull(runningrResponseItem.getFiles());
    }

    @Test
    void generateCgmXmlHeaderFileHeader() throws DatatypeConfigurationException {
        ResponseMessageType responseMessage = new ResponseMessageType();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        raoIXmlResponseGenerator.generateCgmXmlHeaderFileHeader(responseMessage, localDate, correlationId);
        HeaderType header = responseMessage.getHeader();
        assertEquals("created", header.getVerb());
        assertEquals("OptimizedCommonGridModel", header.getNoun());
        assertEquals("1", header.getRevision());
        assertEquals("PRODUCTION", header.getContext());
        assertEquals("22XCORESO------S", header.getSource());
        assertEquals("17XTSO-CS------W", header.getReplyAddress());
        assertEquals("22XCORESO------S-20230804-F304v1", header.getMessageID());
        assertEquals("correlationId", header.getCorrelationID());
    }

    @Test
    void generateCgmXmlHeaderFilePayLoad() {
        ResponseMessageType responseMessage = new ResponseMessageType();
        initTasksForCgmXmlHeader();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        raoIXmlResponseGenerator.generateCgmXmlHeaderFilePayLoad(taskDtos, responseMessage);
        PayloadType payload = responseMessage.getPayload();
        assertEquals(1, payload.getResponseItems().getResponseItem().size());
        ResponseItem responseItem = payload.getResponseItems().getResponseItem().get(0);
        assertEquals("2023-08-04T14:46Z/2023-08-04T15:46Z", responseItem.getTimeInterval());
        assertEquals(1, responseItem.getFiles().getFile().size());
        com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = responseItem.getFiles().getFile().get(0);
        assertEquals("CGM", file.getCode());
        assertEquals("fileName://20230804_1630_2D5_UX1.uct", file.getUrl());
    }
}
