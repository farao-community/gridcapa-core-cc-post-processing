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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Set;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.TEMP_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
@SpringBootTest
class CgmHeaderXmlGeneratorTest {

    private final LocalDate localDate = LocalDate.of(2023, 8, 4);
    private final String startInstantString = "2023-08-04T14:46:00Z";
    private final OffsetDateTime startInstant = OffsetDateTime.parse(startInstantString);
    private final String endInstantString = "2023-08-04T14:47:00Z";
    private final OffsetDateTime endInstant = OffsetDateTime.parse(endInstantString);
    private final String correlationId = "6fe0a389-9315-417e-956d-b3fbaa479caz";
    private Set<TaskDto> taskDtos;

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
            CgmHeaderXmlGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
            Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
            // Second pass with already existing directory
            CgmHeaderXmlGenerator.generateCgmXmlHeaderFile(taskDtos, cgmsArchiveTempPath, localDate, correlationId, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
            Utils.assertFilesContentEqual("/services/CGM_XML_Header.xml", generatedXmlHeaderFile.toString(), true);
            // Delete the temporary directory
            FileUtils.deleteDirectory(new File(generatedXmlHeaderFile.getParent()));
            assertFalse(Files.exists(generatedXmlHeaderFile.getParentFile().toPath()));
        }
    }

    private void initTasksForCgmXmlHeader() {
        TaskDto taskDtoStart = mock(TaskDto.class);
        doReturn(startInstant).when(taskDtoStart).getTimestamp();
        doReturn(TaskStatus.SUCCESS).when(taskDtoStart).getStatus();
        TaskDto taskDtoEnd = mock(TaskDto.class);
        doReturn(endInstant).when(taskDtoEnd).getTimestamp();
        doReturn(TaskStatus.SUCCESS).when(taskDtoEnd).getStatus();
        taskDtos = Set.of(taskDtoStart, taskDtoEnd);
    }

    @Test
    void generateCgmXmlHeaderFileHeader() {
        ResponseMessageType responseMessage = new ResponseMessageType();
        ReflectionTestUtils.invokeMethod(CgmHeaderXmlGenerator.class, "generateCgmXmlHeaderFileHeader", responseMessage, localDate, correlationId);
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
        ReflectionTestUtils.invokeMethod(CgmHeaderXmlGenerator.class, "generateCgmXmlHeaderFilePayLoad", taskDtos, responseMessage, "2023-08-04T14:46:00.000Z/2023-08-04T15:46:00.000Z");
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
