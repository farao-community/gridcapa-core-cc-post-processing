/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.services.DailyFbConstraintGenerator;
import com.farao_community.farao.core_cc_post_processing.app.services.RefProgGenerator;
import com.farao_community.farao.core_cc_post_processing.app.services.ZipAndUploadService;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.CGM_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.CNE_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.RAO_RESULT_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK_CNE_NOT_PRESENT_2019;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostProcessingServiceTest {

    @Mock
    private DailyFbConstraintGenerator dailyFbConstraintGenerator;
    @Mock
    private MinioAdapter minioAdapterMock;
    @Mock
    private RefProgGenerator refProgGenerator;
    @Mock
    private ZipAndUploadService zipAndUploadServiceMock;

    @InjectMocks
    private PostProcessingService postProcessingService;

    private final LocalDate localDate = LocalDate.of(2019, 1, 8);
    private final Set<TaskDto> tasksToPostProcess = Set.of(SUCCESS_TASK_2019);
    private final List<byte[]> logList = new ArrayList<>();
    private final ProcessFileDto metadataProcessFile = new ProcessFileDto("/CORE/CC/coreCCMetadata.json", "METADATA", ProcessFileStatus.VALIDATED, "coreCCMetadata.json", "docId", OffsetDateTime.parse("2019-01-08T12:30Z"));
    private final TaskDto task = new TaskDto(UUID.fromString("00000000-0000-0000-0000-000000000001"), OffsetDateTime.parse("2019-01-08T12:30Z"), TaskStatus.SUCCESS, List.of(metadataProcessFile), List.of(), List.of(), List.of(), List.of(), List.of());

    @Test
    void testProcessTasks() {
        //Given
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/metadata.json"))
            .then(x -> getClass().getResourceAsStream("/services/metadatas/coreCCMetadata.json"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/network.uct"))
//            .then(x -> getClass().getResourceAsStream("/services/network.uct"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/raoResult.json"))
//            .then(x -> getClass().getResourceAsStream("/services/raoResult.json"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/crac.xml"))
//            .then(x -> getClass().getResourceAsStream("/services/f303-1/inputs/F301.xml"));
        final ByteArrayOutputStream baos = mock(ByteArrayOutputStream.class);
        final byte[] bytes = new byte[0];
        when(baos.toByteArray()).thenReturn(bytes);
        when(refProgGenerator.generate(any(), any(), any())).thenReturn(baos);

        //When
        postProcessingService.processTasks(localDate, tasksToPostProcess, logList);

        //Then
        final String expectedTargetMinioFolder = "RAO_OUTPUTS_DIR/2019-01-08";
        final Map<TaskDto, ProcessFileDto> expectedRaoResultPerTask = new HashMap<>();
        expectedRaoResultPerTask.put(SUCCESS_TASK_2019, RAO_RESULT_FILE_DTO_2019);
        final Map<TaskDto, ProcessFileDto> expectedCgmsPerTask = new HashMap<>();
        expectedCgmsPerTask.put(SUCCESS_TASK_2019, CGM_FILE_DTO_2019);
        final Map<TaskDto, ProcessFileDto> expectedCnePerTask = new HashMap<>();
        expectedCnePerTask.put(SUCCESS_TASK_2019, CNE_FILE_DTO_2019);

        verify(zipAndUploadServiceMock).zipRaoResultsAndSendToOutputs(expectedTargetMinioFolder, expectedRaoResultPerTask, localDate);
        verify(zipAndUploadServiceMock).uploadMetadataToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).zipAndUploadLogs(any(), eq(logList), any(), anyInt());
        verify(zipAndUploadServiceMock).zipCgmsAndSendToOutputs(
            expectedTargetMinioFolder,
            expectedCgmsPerTask,
            localDate,
            "00000000-0000-0000-0000-000000000000",
            "2019-01-07T23:00Z/2019-01-08T23:00Z",
            1
        );
        verify(zipAndUploadServiceMock).zipCnesAndSendToOutputs(expectedTargetMinioFolder, expectedCnePerTask, localDate, 1);
        verify(zipAndUploadServiceMock).uploadCbcoraToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).uploadRaoResponseToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).uploadRefProgToMinio(any(), eq(bytes), any(), anyInt());
    }

    @Test
    void testProcessTasksMissingOutputs() {
        //Given
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/metadata.json"))
            .then(x -> getClass().getResourceAsStream("/services/metadatas/coreCCMetadata.json"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/network.uct"))
//            .then(x -> getClass().getResourceAsStream("/services/network.uct"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/raoResult.json"))
//            .then(x -> getClass().getResourceAsStream("/services/raoResult.json"));
//        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/crac.xml"))
//            .then(x -> getClass().getResourceAsStream("/services/f303-1/inputs/F301.xml"));
        final ByteArrayOutputStream baos = mock(ByteArrayOutputStream.class);
        final byte[] bytes = new byte[0];
        when(baos.toByteArray()).thenReturn(bytes);
        when(refProgGenerator.generate(any(), any(), any())).thenReturn(baos);

        //When
        postProcessingService.processTasks(localDate, Set.of(SUCCESS_TASK_CNE_NOT_PRESENT_2019), logList);

        //Then
        final String expectedTargetMinioFolder = "RAO_OUTPUTS_DIR/2019-01-08";
        final Map<TaskDto, ProcessFileDto> expectedRaoResultPerTask = new HashMap<>();
        expectedRaoResultPerTask.put(SUCCESS_TASK_CNE_NOT_PRESENT_2019, RAO_RESULT_FILE_DTO_2019);
        final Map<TaskDto, ProcessFileDto> expectedCgmPerTask = new HashMap<>();
        expectedCgmPerTask.put(SUCCESS_TASK_CNE_NOT_PRESENT_2019, CGM_FILE_DTO_2019);

        verify(zipAndUploadServiceMock).zipRaoResultsAndSendToOutputs(expectedTargetMinioFolder, expectedRaoResultPerTask, localDate);
        verify(zipAndUploadServiceMock).uploadMetadataToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).zipAndUploadLogs(any(), eq(logList), any(), anyInt());
        verify(zipAndUploadServiceMock).zipCgmsAndSendToOutputs(
            expectedTargetMinioFolder,
            expectedCgmPerTask,
            localDate,
            "00000000-0000-0000-0000-000000000000",
            "2019-01-07T23:00Z/2019-01-08T23:00Z",
            1
        );
        //No CNE persisted
        verify(zipAndUploadServiceMock).zipCnesAndSendToOutputs(expectedTargetMinioFolder, Collections.emptyMap(), localDate, 1);
        verify(zipAndUploadServiceMock).uploadCbcoraToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).uploadRaoResponseToMinio(any(), any(), any(), anyInt());
        verify(zipAndUploadServiceMock).uploadRefProgToMinio(any(), eq(bytes), any(), anyInt());
    }

    @Test
    void fetchMetadataFromMinio() {
        final Map<TaskDto, ProcessFileDto> metadatas = Map.of(task, metadataProcessFile);
        when(minioAdapterMock.getFileFromFullPath(anyString())).thenReturn(getClass().getResourceAsStream("/services/metadatas/coreCCMetadata.json"));
        final PostProcessingService.MetadataExtractedFromMinio metadataExtractedFromMinio = postProcessingService.fetchMetadataFromMinio(metadatas);
        final Map<UUID, CoreCCMetadata> metadataMap = metadataExtractedFromMinio.metadataMap();
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
