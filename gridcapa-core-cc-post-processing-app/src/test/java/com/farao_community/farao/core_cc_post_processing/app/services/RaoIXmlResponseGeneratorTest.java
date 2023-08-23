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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.xml.datatype.DatatypeConfigurationException;
import java.io.*;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.*;

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
    private final String correlationId = "6fe0a389-9315-417e-956d-b3fbaa479caz";
    private Set<TaskDto> taskDtos;
    private final String cgmsArchiveTempPath = "/tmp/gridcapa/cgms";
    private final Map<UUID, CoreCCMetadata> metadataMap = new HashMap<>();

    @Autowired
    RaoIXmlResponseGenerator raoIXmlResponseGenerator;

    @Autowired
    MinioAdapter minioAdapter;

    @Test
    void generateCgmXmlHeaderFile() throws IOException {
        initTasksForCgmXmlHeader();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
        File generatedXmlHeaderFile = new File(cgmsArchiveTempPath, "CGM_XML_Header.xml");
        // First pass with not existing directory
        raoIXmlResponseGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId);
        Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
        // Second pass with already existing directory
        raoIXmlResponseGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId);
        Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
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

    @Test
    void generateRaoResponse() throws IOException {
        initTasksForRaoResponse();
        initMetadataMap();
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
        String targetMinioFolder = "/tmp";
        raoIXmlResponseGenerator.generateRaoResponse(taskDtos, targetMinioFolder, localDate, correlationId, metadataMap);
        Utils.assertFilesContentEqual("/services/raoResponse.xml", "/tmp/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml", true);
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
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
        raoIXmlResponseGenerator.generateRaoResponseHeader(responseMessage, localDate, correlationId);
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
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
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
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
        raoIXmlResponseGenerator.generateCgmXmlHeaderFileHeader(responseMessage, localDate, correlationId);
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
        RaoIXmlResponseGenerator raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(Utils.MINIO_FILE_WRITER);
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
