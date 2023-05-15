/*
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.configuration.CoreCCPostProcessingConfiguration;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCFileResource;
import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInvalidDataException;
import com.farao_community.farao.core_cc_post_processing.app.services.FileUtils;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author Ameni Walha {@literal <ameni.walha at rte-france.com>}
 */
@Service
public class CoreCCPostProcessingHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreCCPostProcessingHandler.class);

    private final CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration;
    private final FileUtils fileUtils;
    private final MinioAdapter minioAdapter;
    private final RestTemplateBuilder restTemplateBuilder;
    private final PostProcessingService postProcessingService;

    public CoreCCPostProcessingHandler(CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration, FileUtils fileUtils, MinioAdapter minioAdapter, RestTemplateBuilder restTemplateBuilder, PostProcessingService postProcessingService) {
        this.coreCCPostProcessingConfiguration = coreCCPostProcessingConfiguration;
        this.fileUtils = fileUtils;
        this.minioAdapter = minioAdapter;
        this.restTemplateBuilder = restTemplateBuilder;
        this.postProcessingService = postProcessingService;
    }

    @Bean
    public Consumer<Flux<TaskDto>> consumeTaskDtoUpdate() {
        return f -> f
            .onErrorContinue((t, r) -> LOGGER.error(t.getMessage(), t))
            .subscribe(this::postProcessFinishedTasks);
    }

    private void postProcessFinishedTasks(TaskDto taskDtoUpdated) {
        if (taskDtoUpdated.getStatus().isOver()) {
            // propagate in logs MDC the task id as an extra field to be able to match microservices logs with calculation tasks.
            // This should be done only once, as soon as the information to add in mdc is available.
            LocalDate localDate = taskDtoUpdated.getTimestamp().toLocalDate();
            if (checkIfAllHourlyTasksAreFinished(localDate)) {
                Set<TaskDto> taskDtoForBusinessDate = getAllTaskDtoForBusinessDate(localDate);
                taskDtoForBusinessDate.forEach(taskDto -> {
                    LOGGER.info(taskDto.getTimestamp().toString());
                    LOGGER.info(taskDto.getStatus().toString());
                    LOGGER.info(taskDto.getId().toString());
                });
                postProcessingService.processTasks(localDate, taskDtoForBusinessDate);
                LOGGER.info("YAY! {} tasks found", taskDtoForBusinessDate.size());
            }
        }
    }

    public Set<TaskDto> getAllTaskDtoForBusinessDate(LocalDate localDate) {
        String requestUrl = getUrlToGetAllTasksOfTheDay(localDate);
        LOGGER.info("Requesting URL: {}", requestUrl);

        try {
            ResponseEntity<TaskDto[]> responseEntity = restTemplateBuilder.build().getForEntity(requestUrl, TaskDto[].class);
            if (responseEntity.getBody() != null && responseEntity.getStatusCode() == HttpStatus.OK) {
                LOGGER.info(responseEntity.toString());
                return new HashSet<>(Arrays.asList(responseEntity.getBody()));
            }
        } catch (Exception e) {
            LOGGER.error("Error during automatic launch", e);
        }
        LOGGER.warn("Response entity body was null or status was not OK.");
        return null;
    }

    private String getUrlToGetAllTasksOfTheDay(LocalDate localDate) {
        return coreCCPostProcessingConfiguration.getUrl().getTaskManagerBusinessDateUrl() + localDate;
    }

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

    private String getUrlToCheckAllTasksOfTheDayAreOver(LocalDate localDate) {
        return coreCCPostProcessingConfiguration.getUrl().getTaskManagerBusinessDateUrl() + localDate + "/allOver";
    }

    private static Optional<ProcessFileDto> getProcessFile(TaskDto taskDto, String fileType) {
        return taskDto.getInputs().stream()
            .filter(f -> f.getFileType().equals(fileType))
            .findFirst();
    }

    private CoreCCFileResource getFileResource(TaskDto taskDto, String fileType) {
        return getProcessFile(taskDto, fileType)
            .filter(this::isProcessFileDtoConsistent)
            .map(pfd -> fileUtils.createFileResource(pfd.getFilename(), minioAdapter.generatePreSignedUrlFromFullMinioPath(pfd.getFilePath(), 1)))
            .orElse(null);
    }

    private CoreCCFileResource getFileResourceOrThrow(TaskDto taskDto, String fileType, String referenceCalculationTimeValue) {
        return getProcessFile(taskDto, fileType)
            .filter(this::isProcessFileDtoConsistent)
            .map(pfd -> fileUtils.createFileResource(pfd.getFilename(), minioAdapter.generatePreSignedUrlFromFullMinioPath(pfd.getFilePath(), 1)))
            .orElseThrow(() -> new CoreCCPostProcessingInvalidDataException(String.format("No %s file found in task for timestamp: %s", fileType, referenceCalculationTimeValue)));
    }

    private boolean isProcessFileDtoConsistent(ProcessFileDto processFileDto) {
        return processFileDto.getFilename() != null && processFileDto.getFilePath() != null;
    }
}
