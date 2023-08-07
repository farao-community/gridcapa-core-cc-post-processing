/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.core_cc_post_processing.app.services.RaoIXmlResponseGenerator;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.*;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class PostProcessingServiceTest {

    private MinioAdapter minioAdapter;
    private RaoIXmlResponseGenerator raoIXmlResponseGenerator;
    private DailyF303Generator dailyF303Generator;
    private FlowBasedConstraintDocument dailyFlowBasedConstraintDocument;
    private LocalDate localDate = LocalDate.of(2023, 8, 4);
    private TaskDto taskDto;
    private Set<TaskDto> tasksToPostProcess;
    private List<byte[]> logList = List.of();
    private OffsetDateTime dateTime = OffsetDateTime.of(2023, 8, 4, 16, 39, 00, 0, ZoneId.of("Europe/Brussels").getRules().getOffset(LocalDateTime.now()));
    private String serializedMetadata = "{\"raoRequestFileName\": \"raoRequest.json\", \"requestReceivedInstant\": \"2023-08-04T11:26:00Z\", \"raoRequestInstant\": \"2023-08-04T11:26:00Z\", \"computationStart\": \"2023-08-04T11:27:00Z\", \"computationEnd\": \"2023-08-04T11:29:00Z\", \"timeInterval\": \"2023-08-04T11:25:00Z/2023-08-04T12:25:00Z\", \"correlationId\": \"correlationId\", \"status\": \"SUCCESS\", \"errorCode\": \"0\", \"errorMessage\": \"This is an error.\", \"version\": 1}";
    private boolean logsUploadedToMinio;
    private boolean cgmsUploadedToMinio;
    private boolean cnesUploadedToMinio;
    private boolean dailyOutputFlowBasedConstraintDocumentUploadedToMinio;
    private boolean raoResponseUploadedToMinio;

    @BeforeEach
    void setUp() {
        initUploadBooleans();
        minioAdapter = Mockito.mock(MinioAdapter.class);
        Mockito.doReturn(IOUtils.toInputStream(serializedMetadata)).when(minioAdapter).getFile(Mockito.any());
        Mockito.doAnswer(answer -> logsUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("RAO_OUTPUTS_DIR/2023-08-04/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F342_20230804-F342-01.zip"), Mockito.any());
        Mockito.doAnswer(answer -> cgmsUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("RAO_OUTPUTS_DIR/2023-08-04/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F304_20230804-F304-01.zip"), Mockito.any());
        Mockito.doAnswer(answer -> cnesUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("RAO_OUTPUTS_DIR/2023-08-04/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F299_20230804-F299-01.zip"), Mockito.any());
        Mockito.doAnswer(answer -> dailyOutputFlowBasedConstraintDocumentUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("RAO_OUTPUTS_DIR/2023-08-04/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F303_20230804-F303-01.xml"), Mockito.any());
        Mockito.doAnswer(answer -> raoResponseUploadedToMinio = true).when(minioAdapter).uploadOutput(Mockito.eq("RAO_OUTPUTS_DIR/2023-08-04/outputs/CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F305_20230804-F305-01.xml"), Mockito.any());
    }

    private void initUploadBooleans() {
        logsUploadedToMinio = false;
        cgmsUploadedToMinio = false;
        cnesUploadedToMinio = false;
        dailyOutputFlowBasedConstraintDocumentUploadedToMinio = false;
        raoResponseUploadedToMinio = false;
    }

    private PostProcessingService initPostProcessingService() {
        raoIXmlResponseGenerator = new RaoIXmlResponseGenerator(minioAdapter);
        dailyF303Generator = Mockito.mock(DailyF303Generator.class);
        dailyFlowBasedConstraintDocument = Mockito.mock(FlowBasedConstraintDocument.class);
        Mockito.doReturn(dailyFlowBasedConstraintDocument).when(dailyF303Generator).generate(Mockito.any());
        return new PostProcessingService(minioAdapter, raoIXmlResponseGenerator, dailyF303Generator);
    }

    private void initTask() {
        UUID uuid = UUID.fromString("22711acb-ee59-47ed-b877-3c3688efe820");
        List<ProcessFileDto> inputs = initInputs();
        List<ProcessFileDto> outputs = initOutputs();
        List<ProcessEventDto> processEvents = initProcessEvents();
        taskDto = new TaskDto(uuid, dateTime, TaskStatus.SUCCESS, inputs, outputs, processEvents);
        tasksToPostProcess = Set.of(taskDto);
    }

    private List<ProcessFileDto> initInputs() {
        return List.of();
    }

    private List<ProcessFileDto> initOutputs() {
        ProcessFileDto cneFileDto = new ProcessFileDto("/CORE/CC/cne.xml", "CNE", ProcessFileStatus.VALIDATED, "cne.xml", dateTime);
        ProcessFileDto cgmFileDto = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", dateTime);
        ProcessFileDto metadataFileDto = new ProcessFileDto("/CORE/CC/metadata.csv", "METADATA", ProcessFileStatus.VALIDATED, "metadata.csv", dateTime);
        return List.of(cneFileDto, cgmFileDto, metadataFileDto);
    }

    private List<ProcessEventDto> initProcessEvents() {
        return List.of();
    }

    @Test
    void processTasks() throws IOException {
        initTask();
        PostProcessingService postProcessingService = initPostProcessingService();
        postProcessingService.processTasks(localDate, tasksToPostProcess, logList);
        assertZipDirectoriesExist();
        assertAllUploadsPassed();
        deleteZipDirectories();
    }

    private static void deleteZipDirectories() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/cnes_out"));
        FileUtils.deleteDirectory(new File("/tmp/cgms_out"));
    }

    private static void assertZipDirectoriesExist() {
        assertTrue(Files.exists(Path.of("/tmp/cnes_out")));
        assertTrue(Files.exists(Path.of("/tmp/cgms_out")));
    }

    private void assertAllUploadsPassed() {
        assertTrue(logsUploadedToMinio);
        assertTrue(cgmsUploadedToMinio);
        assertTrue(cnesUploadedToMinio);
        assertTrue(dailyOutputFlowBasedConstraintDocumentUploadedToMinio);
        assertTrue(raoResponseUploadedToMinio);
    }
}
