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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.xml.datatype.DatatypeConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.TEMP_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mockStatic;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
@SpringBootTest
class XmlGeneratorTest {

    private final LocalDate localDate = LocalDate.of(2023, 8, 4);
    private final String startInstantString = "2023-08-04T14:46:00Z";
    private final OffsetDateTime startInstant = OffsetDateTime.parse(startInstantString);
    private final String endInstantString = "2023-08-04T14:47:00Z";
    private final OffsetDateTime endInstant = OffsetDateTime.parse(endInstantString);
    private final String correlationId = "6fe0a389-9315-417e-956d-b3fbaa479caz";
    private Set<TaskDto> taskDtos;
    private final Map<UUID, CoreCCMetadata> metadataMap = new HashMap<>();

    @Autowired
    MinioAdapter minioAdapter;

    @Test
    void generateCgmXmlHeaderFile() throws IOException {
        initTasksForCgmXmlHeader();
        String cgmsArchiveTempPath = TEMP_DIR + "/gridcapa/cgms";
        File generatedXmlHeaderFile = new File(cgmsArchiveTempPath, "CGM_XML_Header.xml");
        // mock instant
        Instant mockedInstant = ZonedDateTime.parse("2023-08-04T12:42:42.000Z").toInstant();
        try (MockedStatic<Instant> mockedStatic = mockStatic(Instant.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatic.when(Instant::now).thenReturn(mockedInstant);
            // First pass with not existing directory
            F305XmlGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
            Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
            // Second pass with already existing directory
            F305XmlGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
            Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
            // Delete the temporary directory
            FileUtils.deleteDirectory(new File(generatedXmlHeaderFile.getParent()));
            assertFalse(Files.exists(generatedXmlHeaderFile.getParentFile().toPath()));
        }
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

    @Test
    void generateRaoResponse() throws IOException {
        initTasksForRaoResponse();
        initMetadataMap();
        // mock instant
        Instant mockedInstant = ZonedDateTime.parse("2023-08-04T12:42:42.000Z").toInstant();
        try (MockedStatic<Instant> mockedStatic = mockStatic(Instant.class, Mockito.CALLS_REAL_METHODS)) {
            mockedStatic.when(Instant::now).thenReturn(mockedInstant);
            final ResponseMessageType raoResponse = F305XmlGenerator.generateRaoResponse(taskDtos, localDate, correlationId, metadataMap, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
            // JSON => OBJECT => JSON to get rid of formatting
            String expectedFileContents = new String(Utils.class.getResourceAsStream("/services/raoResponseMessageType.json").readAllBytes()).replace("\r", "");
            final ObjectMapper mapper = new ObjectMapper();
            final ResponseMessageType parsedFileContent = mapper.readValue(expectedFileContents, ResponseMessageType.class);
            final String reformattedFileContent = mapper.writeValueAsString(parsedFileContent);
            assertEquals(mapper.writeValueAsString(raoResponse), reformattedFileContent);
        }
    }

    private void initTasksForRaoResponse() {
        taskDtos = Set.of(Utils.ERROR_TASK, Utils.RUNNING_TASK, Utils.SUCCESS_TASK);
    }

    private void initMetadataMap() {
        metadataMap.put(Utils.ERROR_TASK.getId(), Utils.CORE_CC_METADATA_ERROR);
        metadataMap.put(Utils.RUNNING_TASK.getId(), Utils.CORE_CC_METADATA_RUNNING);
        metadataMap.put(Utils.SUCCESS_TASK.getId(), Utils.CORE_CC_METADATA_SUCCESS);
    }

    @Test
    void generateRaoResponseHeader() throws DatatypeConfigurationException {
        ResponseMessageType responseMessage = new ResponseMessageType();
        F305XmlGenerator.generateRaoResponseHeader(responseMessage, localDate, correlationId);
        HeaderType header = responseMessage.getHeader();
        assertEquals("created", header.getVerb());
        assertEquals("OptimizedRemedialActions", header.getNoun());
        assertEquals("1", header.getRevision());
        assertEquals("PRODUCTION", header.getContext());
        assertEquals("22XCORESO------S", header.getSource());
        assertEquals("17XTSO-CS------W", header.getReplyAddress());
        assertEquals("22XCORESO------S-20230804-F305", header.getMessageID());
        assertEquals("6fe0a389-9315-417e-956d-b3fbaa479caz", header.getCorrelationID());
    }

    @Test
    void generateRaoResponsePayLoad() {
        ResponseMessageType responseMessage = new ResponseMessageType();
        initTasksForRaoResponse();
        initMetadataMap();
        F305XmlGenerator.generateRaoResponsePayLoad(taskDtos, responseMessage, localDate, metadataMap, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
        PayloadType payload = responseMessage.getPayload();

        assertEquals(3, payload.getResponseItems().getResponseItem().size());

        ResponseItem successResponseItem = payload.getResponseItems().getResponseItem().get(0);
        assertEquals("2023-08-21T14:46Z/2023-08-21T15:46Z", successResponseItem.getTimeInterval());
        assertEquals(3, successResponseItem.getFiles().getFile().size());
        assertEquals("OPTIMIZED_CGM", successResponseItem.getFiles().getFile().get(0).getCode());
        assertEquals("documentIdentification://22XCORESO------S-20230804-F304v1", successResponseItem.getFiles().getFile().get(0).getUrl());
        assertEquals("OPTIMIZED_CB", successResponseItem.getFiles().getFile().get(1).getCode());
        assertEquals("documentIdentification://22XCORESO------S-20230804-F303v1", successResponseItem.getFiles().getFile().get(1).getUrl());
        assertEquals("RAO_REPORT", successResponseItem.getFiles().getFile().get(2).getCode());
        assertEquals("documentIdentification://22XCORESO------S-20230804-F299v1", successResponseItem.getFiles().getFile().get(2).getUrl());

        ResponseItem errorResponseItem = payload.getResponseItems().getResponseItem().get(1);
        assertEquals("2023-08-21T14:46Z/2023-08-21T15:46Z", errorResponseItem.getTimeInterval());
        assertEquals("1", errorResponseItem.getError().getCode());
        assertEquals("FATAL", errorResponseItem.getError().getLevel());
        assertNull(errorResponseItem.getFiles());
    }

    @Test
    void generateCgmXmlHeaderFileHeader() throws DatatypeConfigurationException {
        ResponseMessageType responseMessage = new ResponseMessageType();
        F305XmlGenerator.generateCgmXmlHeaderFileHeader(responseMessage, localDate, correlationId);
        HeaderType header = responseMessage.getHeader();
        assertEquals("created", header.getVerb());
        assertEquals("OptimizedCommonGridModel", header.getNoun());
        assertEquals("1", header.getRevision());
        assertEquals("PRODUCTION", header.getContext());
        assertEquals("22XCORESO------S", header.getSource());
        assertEquals("17XTSO-CS------W", header.getReplyAddress());
        assertEquals("22XCORESO------S-20230804-F304v1", header.getMessageID());
        assertEquals("6fe0a389-9315-417e-956d-b3fbaa479caz", header.getCorrelationID());
    }

    @Test
    void generateCgmXmlHeaderFilePayLoad() {
        ResponseMessageType responseMessage = new ResponseMessageType();
        initTasksForCgmXmlHeader();
        F305XmlGenerator.generateCgmXmlHeaderFilePayLoad(taskDtos, responseMessage, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
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
