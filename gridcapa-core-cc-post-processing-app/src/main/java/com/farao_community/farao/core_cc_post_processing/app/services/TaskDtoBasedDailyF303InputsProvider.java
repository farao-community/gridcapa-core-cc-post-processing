/*
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintCreationContext;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintImporter;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;

import static com.farao_community.farao.core_cc_post_processing.app.util.CracUtil.importNativeCrac;

/**
 * @author Sebastien Murgey {@literal <sebastien.murgey at rte-france.com>}
 */
public class TaskDtoBasedDailyF303InputsProvider implements DailyF303GeneratorInputsProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDtoBasedDailyF303InputsProvider.class);
    public static final String CRAC_CREATION_PARAMETERS_JSON = "/crac/cracCreationParameters.json";
    private final Set<TaskDto> tasks;
    private final MinioAdapter minioAdapter;

    public TaskDtoBasedDailyF303InputsProvider(Set<TaskDto> tasks, MinioAdapter minioAdapter) {
        this.tasks = tasks;
        this.minioAdapter = minioAdapter;
    }

    @Override
    public FlowBasedConstraintDocument referenceConstraintDocument() {
        String cracFilePath = tasks.stream()
                .findFirst().orElseThrow()
                .getInputs()
                .stream().filter(processFileDto -> processFileDto.getFileType().equals("CBCORA"))
                .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException("task dto missing cbcora file"))
                .getFilePath();
        return getFlowBasedConstraintDocument(cracFilePath);
    }

    @Override
    public Optional<HourlyF303InfoGenerator.Inputs> hourlyF303InputsForInterval(Interval interval) {
        Optional<TaskDto> taskDtoOptional = getTaskDtoOfInterval(interval, tasks);
        if (taskDtoOptional.isPresent()) {
            TaskDto taskDto = taskDtoOptional.get();
            if (!taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
                return Optional.empty();
            }
            try {
                Network network = getNetworkOfTaskDto(taskDto, minioAdapter);
                FbConstraintCreationContext crac = getCracOfTaskDto(network, taskDto, minioAdapter);
                RaoResult raoResult = getRaoResultOfTaskDto(crac, taskDto, minioAdapter);
                return Optional.of(new HourlyF303InfoGenerator.Inputs(crac, raoResult, taskDto.getTimestamp()));
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean shouldBeReported(Interval interval) {
        Optional<TaskDto> taskDtoOptional = getTaskDtoOfInterval(interval, tasks);
        return taskDtoOptional.isPresent() && taskDtoOptional.get().getStatus().equals(TaskStatus.SUCCESS);
    }

    private Optional<TaskDto> getTaskDtoOfInterval(Interval interval, Set<TaskDto> taskDtos) {
        return taskDtos.stream().filter(taskDto -> interval.contains(taskDto.getTimestamp().toInstant())).findFirst();
    }

    private CracCreationParameters getCimCracCreationParameters() {
        LOGGER.info("Importing Crac Creation Parameters file: {}", CRAC_CREATION_PARAMETERS_JSON);
        return JsonCracCreationParameters.read(getClass().getResourceAsStream(CRAC_CREATION_PARAMETERS_JSON));
    }

    private FlowBasedConstraintDocument getFlowBasedConstraintDocument(String cracFilePath) {
        FlowBasedConstraintDocument flowBasedConstraintDocument;
        try (final InputStream cracXmlInputStream = minioAdapter.getFileFromFullPath(cracFilePath)) {
            flowBasedConstraintDocument = importNativeCrac(cracXmlInputStream);
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during F303 file creation", e);
        }
        return flowBasedConstraintDocument;
    }

    private Network getNetworkOfTaskDto(TaskDto taskDto, MinioAdapter minioAdapter) {
        Optional<ProcessFileDto> processFileDto = taskDto.getOutputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("CGM_OUT"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot find network of task %s", taskDto.getTimestamp()));
        }
        try (InputStream networkInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return Network.read(processFileDto.get().getFilename(), networkInputStream);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import network of task %s", taskDto.getTimestamp()), e);
        }
    }

    private FbConstraintCreationContext getCracOfTaskDto(Network network, TaskDto taskDto, MinioAdapter minioAdapter) {
        Optional<ProcessFileDto> processFileDto = taskDto.getInputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("CBCORA"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot find crac of task %s", taskDto.getTimestamp()));
        }
        CracCreationParameters cracCreationParameters = getCimCracCreationParameters();
        try (InputStream cracInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return (FbConstraintCreationContext) new FbConstraintImporter().importData(cracInputStream, cracCreationParameters, network, taskDto.getTimestamp());
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import crac of task %s", taskDto.getTimestamp()), e);
        }
    }

    private RaoResult getRaoResultOfTaskDto(FbConstraintCreationContext crac, TaskDto taskDto, MinioAdapter minioAdapter) {
        Optional<ProcessFileDto> processFileDto = taskDto.getOutputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("RAO_RESULT"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import RAO result of task %s", taskDto.getTimestamp()));
        }
        try (InputStream raoResultInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return RaoResult.read(raoResultInputStream, crac.getCrac());
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import RAO result of hourly RAO response of instant %s", taskDto.getTimestamp()), e);
        }
    }
}
