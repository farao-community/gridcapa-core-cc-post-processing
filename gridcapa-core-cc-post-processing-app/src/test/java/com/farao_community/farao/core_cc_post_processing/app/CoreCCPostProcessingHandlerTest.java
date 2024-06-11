/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.configuration.CoreCCPostProcessingConfiguration;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Ameni Walha {@literal <ameni.walha at rte-france.com>}
 */
@SpringBootTest
class CoreCCPostProcessingHandlerTest {

    private final CoreCCPostProcessingConfiguration.UrlProperties url = new CoreCCPostProcessingConfiguration.UrlProperties("http://mockUrl/2023-08-21T11_26_00/", "/2023-08-21/");
    private final CoreCCPostProcessingConfiguration.ProcessProperties properties = new CoreCCPostProcessingConfiguration.ProcessProperties("tag", "Europe/Brussels");
    private final CoreCCPostProcessingConfiguration configuration = new CoreCCPostProcessingConfiguration(url, properties);

    @Autowired
    private CoreCCPostProcessingHandler coreCCPostProcessingHandler;

    private final RestTemplateBuilder restTemplateBuilder = Mockito.mock(RestTemplateBuilder.class);
    private final PostProcessingService postProcessingService = Mockito.mock(PostProcessingService.class);
    private boolean tasksProcessed = false;

    void initCoreCCPostProcessingHandler() {
        coreCCPostProcessingHandler = new CoreCCPostProcessingHandler(configuration, restTemplateBuilder, postProcessingService);
    }

    @Test
    void getLogsForTask() {
        ResponseEntity responseEntity = Mockito.mock(ResponseEntity.class);
        Mockito.when(responseEntity.getBody()).thenReturn("Hello world!".getBytes(StandardCharsets.UTF_8));
        Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplate.getForEntity("http://mockUrl/2023-08-21T11_26_00/2023-08-21T15:16:45Z/log", byte[].class)).thenReturn(responseEntity);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplate);
        initCoreCCPostProcessingHandler();
        List<byte[]> logList = coreCCPostProcessingHandler.getLogsForTask(Set.of(Utils.SUCCESS_TASK));
        assertEquals(1, logList.size());
        assertEquals("Hello world!", new String(logList.get(0)));
    }

    @Test
    void getLogsForTaskWithError() {
        // Without further mock, a null exception is thrown
        List<byte[]> logList = coreCCPostProcessingHandler.getLogsForTask(Set.of(Utils.SUCCESS_TASK));
        assertEquals(0, logList.size());
    }

    @Test
    void getLogsForTaskWithInternalServerError() {
        ResponseEntity responseEntity = Mockito.mock(ResponseEntity.class);
        Mockito.when(responseEntity.getBody()).thenReturn("Hello world!".getBytes(StandardCharsets.UTF_8));
        Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
        RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplate.getForEntity("http://mockUrl/2023-08-21T11_26_00/2023-08-21T15:16:45Z/log", byte[].class)).thenReturn(responseEntity);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplate);
        initCoreCCPostProcessingHandler();
        List<byte[]> logList = coreCCPostProcessingHandler.getLogsForTask(Set.of(Utils.SUCCESS_TASK));
        assertEquals(0, logList.size());
    }

    @Test
    void getAllTaskDtoForBusinessDate() {
        LocalDate localDate = LocalDate.of(2023, 8, 21);

        ResponseEntity responseEntity = Mockito.mock(ResponseEntity.class);
        TaskDto[] taskToRetrieve = new TaskDto[]{Utils.SUCCESS_TASK, Utils.ERROR_TASK};
        Mockito.when(responseEntity.getBody()).thenReturn(taskToRetrieve);
        Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
        RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplate.getForEntity("/2023-08-21/2023-08-21", TaskDto[].class)).thenReturn(responseEntity);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplate);

        initCoreCCPostProcessingHandler();
        Set<TaskDto> tasks = coreCCPostProcessingHandler.getAllTaskDtoForBusinessDate(localDate);

        assertEquals(2, tasks.size());

        TaskDto taskSuccess = tasks.stream().filter(task -> task.getStatus() == TaskStatus.SUCCESS).collect(Collectors.toList()).get(0);
        assertEquals(TaskStatus.SUCCESS, taskSuccess.getStatus());
        assertEquals("4fb56583-bcec-4ed9-9839-0984b7324989", taskSuccess.getId().toString());
        assertEquals("2023-08-21T15:16:45Z", taskSuccess.getTimestamp().toString());

        TaskDto taskError = tasks.stream().filter(task -> task.getStatus() == TaskStatus.ERROR).collect(Collectors.toList()).get(0);
        assertEquals(TaskStatus.ERROR, taskError.getStatus());
        assertEquals("6e3e0ef2-96e4-4649-82d4-374f103038d4", taskError.getId().toString());
        assertEquals("2023-08-21T15:16:46Z", taskError.getTimestamp().toString());
    }

    @Test
    void getAllTaskDtoForBusinessDateWithError() {
        // Without further mock, a null exception is thrown
        LocalDate localDate = LocalDate.of(2023, 8, 21);
        Set<TaskDto> tasks = coreCCPostProcessingHandler.getAllTaskDtoForBusinessDate(localDate);
        assertEquals(0, tasks.size());
    }

    @Test
    void getAllTaskDtoForBusinessDateWithInternalServerError() {
        LocalDate localDate = LocalDate.of(2023, 8, 21);

        ResponseEntity responseEntity = Mockito.mock(ResponseEntity.class);
        TaskDto[] taskToRetrieve = new TaskDto[]{Utils.SUCCESS_TASK, Utils.ERROR_TASK};
        Mockito.when(responseEntity.getBody()).thenReturn(taskToRetrieve);
        Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
        RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplate.getForEntity("/2023-08-21/2023-08-21", TaskDto[].class)).thenReturn(responseEntity);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplate);

        initCoreCCPostProcessingHandler();
        Set<TaskDto> tasks = coreCCPostProcessingHandler.getAllTaskDtoForBusinessDate(localDate);

        assertEquals(0, tasks.size());
    }

    @Test
    void testLocalDateOfFinishedTasks() {
        //First timestamp of the 00h30 locale
        final OffsetDateTime firstTsOfDay = OffsetDateTime.of(2024, 6, 10, 22, 30, 0, 0, ZoneOffset.UTC);
        //23h30 locale
        final OffsetDateTime lastTsOfDay = OffsetDateTime.of(2024, 6, 11, 21, 30, 0, 0, ZoneOffset.UTC);
        final TaskDto firstTaskOfDay = new TaskDto(null, firstTsOfDay, TaskStatus.SUCCESS, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        final TaskDto lastTaskOfDay = new TaskDto(null, lastTsOfDay, TaskStatus.SUCCESS, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        final RestTemplate mockRestTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplateBuilder.build()).thenReturn(mockRestTemplate);
        initCoreCCPostProcessingHandler();
        coreCCPostProcessingHandler.postProcessFinishedTasks(firstTaskOfDay);
        coreCCPostProcessingHandler.postProcessFinishedTasks(lastTaskOfDay);
        Mockito.verify(mockRestTemplate, Mockito.times(2))
                .getForEntity("/2023-08-21/2024-06-11/allOver", Boolean.class);
    }

    @Test
    void postProcessFinishedTasks() {
        LocalDate localDate = LocalDate.of(2023, 8, 21);
        TaskDto[] tasks = new TaskDto[]{Utils.SUCCESS_TASK, Utils.ERROR_TASK, Utils.RUNNING_TASK};
        Set<TaskDto> tasksAsSet = new HashSet<>(Arrays.asList(tasks));
        initCoreCCPostProcessingHandler();

        ResponseEntity responseEntityTasksNotFinished = Mockito.mock(ResponseEntity.class);
        Mockito.when(responseEntityTasksNotFinished.getBody()).thenReturn(false);
        Mockito.when(responseEntityTasksNotFinished.getStatusCode()).thenReturn(HttpStatus.OK);
        RestTemplate restTemplateTasksNotFinished = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplateTasksNotFinished.getForEntity("/2023-08-21/2023-08-21", Boolean.class)).thenReturn(responseEntityTasksNotFinished);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplateTasksNotFinished);

        Mockito.doAnswer(ans -> tasksProcessed = true).when(postProcessingService).processTasks(Mockito.eq(localDate), Mockito.eq(tasksAsSet), Mockito.any());

        // Running task is not over
        coreCCPostProcessingHandler.postProcessFinishedTasks(Utils.RUNNING_TASK);
        assertFalse(tasksProcessed);

        // Success task is over but all tasks not finished
        coreCCPostProcessingHandler.postProcessFinishedTasks(Utils.SUCCESS_TASK);
        assertFalse(tasksProcessed);

        ResponseEntity responseEntityTasksFinishedBoolean = Mockito.mock(ResponseEntity.class);
        Mockito.when(responseEntityTasksFinishedBoolean.getBody()).thenReturn(true);
        Mockito.when(responseEntityTasksFinishedBoolean.getStatusCode()).thenReturn(HttpStatus.OK);

        ResponseEntity responseEntityTasksFinishedTasks = Mockito.mock(ResponseEntity.class);
        Mockito.when(responseEntityTasksFinishedTasks.getBody()).thenReturn(tasks);
        Mockito.when(responseEntityTasksFinishedTasks.getStatusCode()).thenReturn(HttpStatus.OK);

        RestTemplate restTemplateTasksFinished = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplateTasksFinished.getForEntity("/2023-08-21/2023-08-21/allOver", Boolean.class)).thenReturn(responseEntityTasksFinishedBoolean);
        Mockito.when(restTemplateTasksFinished.getForEntity("/2023-08-21/2023-08-21", TaskDto[].class)).thenReturn(responseEntityTasksFinishedTasks);

        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplateTasksFinished);

        // Error task is over and all tasks are finished
        coreCCPostProcessingHandler.postProcessFinishedTasks(Utils.ERROR_TASK);
        assertTrue(tasksProcessed);
    }
}
