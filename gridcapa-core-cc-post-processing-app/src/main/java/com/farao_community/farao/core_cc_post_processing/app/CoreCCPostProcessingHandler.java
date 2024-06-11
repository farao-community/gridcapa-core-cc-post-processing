/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.configuration.CoreCCPostProcessingConfiguration;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public class CoreCCPostProcessingHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreCCPostProcessingHandler.class);
    private final CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration;
    private final RestTemplateBuilder restTemplateBuilder;
    private final PostProcessingService postProcessingService;

    public CoreCCPostProcessingHandler(CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration, RestTemplateBuilder restTemplateBuilder, PostProcessingService postProcessingService) {
        this.coreCCPostProcessingConfiguration = coreCCPostProcessingConfiguration;
        this.restTemplateBuilder = restTemplateBuilder;
        this.postProcessingService = postProcessingService;
    }

    /**
     * Trigger postProcessFinishedTasks every time a task is updated
     */
    @Bean
    public Consumer<Flux<TaskDto>> consumeTaskDtoUpdate() {
        return f -> f
            .onErrorContinue((t, r) -> LOGGER.error(t.getMessage(), t))
            .subscribe(this::postProcessFinishedTasks);
    }

    /**
     * Launch processTasks if all tasks associated to localDate are finished
     */
    void postProcessFinishedTasks(TaskDto taskDtoUpdated) {
        try {
            if (taskDtoUpdated.getStatus().isOver()) {
                // propagate in logs MDC the task id as an extra field to be able to match microservices logs with calculation tasks.
                // This should be done only once, as soon as the information to add in mdc is available.
                LocalDate localDate = taskDtoUpdated.getTimestamp().atZoneSameInstant(ZoneId.of("CET")).toLocalDate();
                if (checkIfAllHourlyTasksAreFinished(localDate)) {
                    Set<TaskDto> taskDtoForBusinessDate = getAllTaskDtoForBusinessDate(localDate);
                    // Only perform post processing if a task from local date was updated
                    if (taskDtoForBusinessDate.stream().map(TaskDto::getId).anyMatch(uuid -> uuid.equals(taskDtoUpdated.getId()))) {
                        postProcessingService.processTasks(localDate, taskDtoForBusinessDate, getLogsForTask(taskDtoForBusinessDate));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * Gather all finished tasks associated to localDate by requesting TaskManager
     * A task is finished when TaskStats::isOver is true
     */
    private boolean checkIfAllHourlyTasksAreFinished(LocalDate localDate) {
        String requestUrl = getUrlToCheckAllTasksOfTheDayAreOver(localDate);
        try {
            ResponseEntity<Boolean> responseEntity = restTemplateBuilder.build().getForEntity(requestUrl, Boolean.class);
            if (responseEntity.getBody() != null && responseEntity.getStatusCode() == HttpStatus.OK) {
                return responseEntity.getBody();
            }
        } catch (Exception e) {
            LOGGER.error("Error while checking if all hourly tasks are finished.", e);
        }
        return false;
    }

    /**
     * Gather the set of tasks associated to localDate by requesting TaskManager
     */
    public Set<TaskDto> getAllTaskDtoForBusinessDate(LocalDate localDate) {
        String requestUrl = getUrlToGetAllTasksOfTheDay(localDate);
        LOGGER.info("Requesting URL: {}", requestUrl);

        try {
            ResponseEntity<TaskDto[]> responseEntity = restTemplateBuilder.build().getForEntity(requestUrl, TaskDto[].class);
            if (responseEntity.getBody() != null && responseEntity.getStatusCode() == HttpStatus.OK) {
                return new HashSet<>(Arrays.asList(responseEntity.getBody()));
            }
        } catch (Exception e) {
            LOGGER.error("Error during automatic launch", e);
        }
        LOGGER.warn("Response entity body was null or status was not OK.");
        return Collections.emptySet();
    }

    private String getUrlToCheckAllTasksOfTheDayAreOver(LocalDate localDate) {
        return coreCCPostProcessingConfiguration.getUrl().getTaskManagerBusinessDateUrl() + localDate + "/allOver";
    }

    private String getUrlToGetAllTasksOfTheDay(LocalDate localDate) {
        return coreCCPostProcessingConfiguration.getUrl().getTaskManagerBusinessDateUrl() + localDate;
    }

    /**
     * Gather logs associated to a set of tasks
     */
    public List<byte[]> getLogsForTask(Set<TaskDto> taskList) {
        List<byte[]> logList = new ArrayList<>();
        taskList.forEach(taskDto -> {
            String offsetDateTime = taskDto.getTimestamp().toString();
            String requestUrl = getUrlToExportTaskLog(offsetDateTime);
            try {
                ResponseEntity<byte[]> responseEntity = restTemplateBuilder.build().getForEntity(requestUrl, byte[].class);
                if (responseEntity.getBody() != null && responseEntity.getStatusCode() == HttpStatus.OK) {
                    logList.add(Objects.requireNonNull(responseEntity.getBody()));
                }
            } catch (Exception e) {
                LOGGER.error("Error while getting log for timestamp {}.", offsetDateTime, e);
            }
        });
        return logList;
    }

    private String getUrlToExportTaskLog(String offsetDateTime) {
        return coreCCPostProcessingConfiguration.getUrl().getTaskManagerTimestampUrl() + offsetDateTime + "/log";
    }
}
