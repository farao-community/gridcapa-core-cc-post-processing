/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class RaoIXmlResponseGeneratorTest {

    private MinioAdapter minioAdapter;
    private String targetMinioFolder = "/minioFolder";
    private LocalDate localDate = LocalDate.of(2023, 8, 4);
    private String startInstantString = "2023-08-04T14:46:00Z";
    private OffsetDateTime startInstant = OffsetDateTime.parse(startInstantString);
    private String endInstantString = "2023-08-04T14:47:00Z";
    private OffsetDateTime endInstant = OffsetDateTime.parse(endInstantString);
    private String correlationId = "correlationId";
    private TaskDto taskDtoStart;
    private TaskDto taskDtoEnd;
    private TaskDto errorTask;
    private TaskDto runningTask;
    private TaskDto successTask;
    private Set<TaskDto> taskDtos;
    private String cgmsArchiveTempPath = "/tmp/gridcapa/cgms";
    private String xmlHeaderFileName = "/services/CGM_XML_Header.xml";
    private Path xmlHeaderFilePath = Paths.get(getClass().getResource(xmlHeaderFileName).getPath());
    private File xmlHeaderFile = new File(xmlHeaderFilePath.toString());
    private boolean raoResponseIsUploadedToMinio;
    private CoreCCMetadata coreCCMetadataErrorTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "SUCCESS", "0", "This is an error.", 1);
    private CoreCCMetadata coreCCMetadataRunningTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "SUCCESS", "0", "This is an error.", 1);
    private CoreCCMetadata coreCCMetadataSuccessTask = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "correlationId", "SUCCESS", "0", "This is an error.", 1);
    private UUID uuidErrorTask = UUID.fromString("22711acb-ee59-47ed-b877-3c3688efe820");
    private UUID uuidRunningTask = UUID.fromString("259ebbe3-4639-4fa6-9687-cdb38a2f36cc");
    private UUID uuidSuccessTask = UUID.fromString("6df6092d-145b-47be-b412-54fc45a90a04");
    private Map<UUID, CoreCCMetadata> metadataMap = new HashMap<UUID, CoreCCMetadata>();
    private ProcessFileDto processFileDto = new ProcessFileDto("/path/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", OffsetDateTime.of(2023, 8, 4, 16, 39, 00, 0, ZoneId.of("Europe/Brussels").getRules().getOffset(LocalDateTime.now())));

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
        taskDtoStart = Mockito.mock(TaskDto.class);
        Mockito.doReturn(startInstant).when(taskDtoStart).getTimestamp();
        Mockito.doReturn(TaskStatus.SUCCESS).when(taskDtoStart).getStatus();
        taskDtoEnd = Mockito.mock(TaskDto.class);
        Mockito.doReturn(endInstant).when(taskDtoEnd).getTimestamp();
        Mockito.doReturn(TaskStatus.SUCCESS).when(taskDtoEnd).getStatus();
        taskDtos = Set.of(taskDtoStart, taskDtoEnd);
    }

    private void changeDateInGeneratedCggmXmlHeader(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String pattern = "<Timestamp>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z<\\/Timestamp>";
        String replaceBy = "<Timestamp>2023-08-04T13:03:23.900189Z</Timestamp>";
        String line;
        String oldText = "";
        while ((line = reader.readLine()) != null) {
            oldText += line + "\r\n";
        }
        reader.close();
        String newText = oldText.replaceAll(pattern, replaceBy);
        FileWriter writer = new FileWriter(file.getAbsolutePath().toString());
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
        raoIXmlResponseGenerator.generateRaoResponse(taskDtos, targetMinioFolder, localDate, correlationId, metadataMap);
        assertTrue(raoResponseIsUploadedToMinio);
    }

    private void initTasksForRaoResponse() {
        errorTask = Mockito.mock(TaskDto.class);
        Mockito.doReturn(startInstant).when(errorTask).getTimestamp();
        Mockito.doReturn(TaskStatus.ERROR).when(errorTask).getStatus();
        Mockito.doReturn(uuidErrorTask).when(errorTask).getId();
        Mockito.doReturn(List.of(processFileDto)).when(errorTask).getOutputs();
        runningTask = Mockito.mock(TaskDto.class);
        Mockito.doReturn(startInstant).when(runningTask).getTimestamp();
        Mockito.doReturn(TaskStatus.RUNNING).when(runningTask).getStatus();
        Mockito.doReturn(uuidRunningTask).when(runningTask).getId();
        Mockito.doReturn(List.of(processFileDto)).when(runningTask).getOutputs();
        successTask = Mockito.mock(TaskDto.class);
        Mockito.doReturn(startInstant).when(successTask).getTimestamp();
        Mockito.doReturn(TaskStatus.SUCCESS).when(successTask).getStatus();
        Mockito.doReturn(uuidSuccessTask).when(successTask).getId();
        Mockito.doReturn(List.of(processFileDto)).when(successTask).getOutputs();
        taskDtos = Set.of(errorTask, runningTask, successTask);
        Mockito.doReturn(List.of(processFileDto)).when(errorTask).getOutputs();
    }

    private void initMetadataMap() {
        metadataMap.put(uuidErrorTask, coreCCMetadataErrorTask);
        metadataMap.put(uuidRunningTask, coreCCMetadataRunningTask);
        metadataMap.put(uuidSuccessTask, coreCCMetadataSuccessTask);
    }
}
