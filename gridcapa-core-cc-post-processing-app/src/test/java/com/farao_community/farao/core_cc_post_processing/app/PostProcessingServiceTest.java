/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.services.ZipAndUploadService;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.CGM_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.CNE_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.RAO_RESULT_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostProcessingServiceTest {

    @Mock
    private MinioAdapter minioAdapterMock;

    @Mock
    private ZipAndUploadService zipAndUploadServiceMock;

    @InjectMocks
    private PostProcessingService postProcessingService;

    private final LocalDate localDate = LocalDate.of(2023, 8, 4);
    private final Set<TaskDto> tasksToPostProcess = Set.of(SUCCESS_TASK);
    private final List<byte[]> logList = new ArrayList<>();
    private final InputStream inputMetadataInputStream = getClass().getResourceAsStream("/services/metadatas/coreCCMetadata.json");
    InputStream inputCracXmlInputStream = getClass().getResourceAsStream("/services/f303-1/inputs/F301.xml");
    private final ProcessFileDto metadataProcessFile = new ProcessFileDto("/CORE/CC/coreCCMetadata.json", "METADATA", ProcessFileStatus.VALIDATED, "coreCCMetadata.json", OffsetDateTime.parse("2019-01-08T12:30Z"));
    private final TaskDto task = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000001"), OffsetDateTime.parse("2019-01-08T12:30Z"), TaskStatus.SUCCESS, List.of(metadataProcessFile), List.of(), List.of(), List.of());

    @Test
    void testProcessTasks() {
        //Given
        when(minioAdapterMock.getFileFromFullPath(ArgumentMatchers.anyString()))
                .thenReturn(inputMetadataInputStream);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/crac.xml"))
                .thenReturn(inputCracXmlInputStream);

        //When
        postProcessingService.processTasks(localDate, tasksToPostProcess, logList);

        //Then
        final String expectedTargetMinioFolder = "RAO_OUTPUTS_DIR/2023-08-04";
        final Map<TaskDto, ProcessFileDto> expectedRaoResultPerTask = new HashMap<>();
        expectedRaoResultPerTask.put(SUCCESS_TASK, RAO_RESULT_FILE_DTO);
        final Map<TaskDto, ProcessFileDto> expectedCgmsPerTask = new HashMap<>();
        expectedCgmsPerTask.put(SUCCESS_TASK, CGM_FILE_DTO);
        final Map<TaskDto, ProcessFileDto> expectedCnePerTask = new HashMap<>();
        expectedCnePerTask.put(SUCCESS_TASK, CNE_FILE_DTO);

        verify(zipAndUploadServiceMock)
                .zipRaoResultsAndSendToOutputs(expectedTargetMinioFolder, expectedRaoResultPerTask, localDate);
        verify(zipAndUploadServiceMock).uploadF341ToMinio(any(), any(), any());
        verify(zipAndUploadServiceMock)
                .zipAndUploadLogs(logList, "RAO_OUTPUTS_DIR/2023-08-04/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-342_20190108-F342-01.zip");
        verify(zipAndUploadServiceMock)
                .zipCgmsAndSendToOutputs(expectedTargetMinioFolder, expectedCgmsPerTask, localDate, "00000000-0000-0000-0000-000000000000", "2019-01-07T23:00Z/2019-01-08T23:00Z");
        verify(zipAndUploadServiceMock)
                .zipCnesAndSendToOutputs(expectedTargetMinioFolder, expectedCnePerTask, localDate);
        verify(zipAndUploadServiceMock).uploadF303ToMinio(any(), any(), any());
        verify(zipAndUploadServiceMock).uploadF305ToMinio(any(), any(), any());
    }

    @Test
    void fetchMetadataFromMinio() {
        // Process file with NOT_CREATED floaf -> will be filtered out
        final ProcessFileDto runningMetadataProcessFile = new ProcessFileDto("/CORE/CC/coreCCMetadataRunning.json", "METADATA", ProcessFileStatus.NOT_PRESENT, "coreCCMetadataRunning.json", OffsetDateTime.parse("2019-01-08T12:30Z"));
        final TaskDto taskRunning = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000002"), OffsetDateTime.parse("2019-01-08T13:30Z"), TaskStatus.RUNNING, List.of(runningMetadataProcessFile), List.of(), List.of(), List.of());
        final Map<TaskDto, ProcessFileDto> metadatas = Map.of(task, metadataProcessFile, taskRunning, runningMetadataProcessFile);
        when(minioAdapterMock.getFileFromFullPath(anyString()))
                .thenReturn(inputMetadataInputStream);
        final Map<UUID, CoreCCMetadata> metadataMap = postProcessingService.fetchMetadataFromMinio(metadatas);
        assertEquals(1, metadataMap.size());

        final CoreCCMetadata metadata = metadataMap.get(UUID.fromString("00000000-0000-0000-0000-000000000001"));
        assertEquals("raoRequest.json", metadata.getRaoRequestFileName());
        assertEquals("2019-01-08T12:30:00Z", metadata.getRequestReceivedInstant());
        assertEquals("2019-01-08T12:30:00Z", metadata.getRaoRequestInstant());
        assertEquals("2019-01-08T12:30:00Z", metadata.getComputationStart());
        assertEquals("2019-01-08T12:31:00Z", metadata.getComputationEnd());
        assertEquals("2019-01-07T23:00Z/2019-01-08T23:00Z", metadata.getTimeInterval());
        assertEquals("00000000-0000-0000-0000-000000000000", metadata.getCorrelationId());
        assertEquals("SUCCESS", metadata.getStatus());
        assertEquals("0", metadata.getErrorCode());
        assertEquals("This is an error.", metadata.getErrorMessage());
        assertEquals(1, metadata.getVersion());
    }
}
