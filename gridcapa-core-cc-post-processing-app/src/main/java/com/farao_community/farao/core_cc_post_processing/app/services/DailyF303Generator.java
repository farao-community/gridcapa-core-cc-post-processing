/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
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
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.farao_community.farao.core_cc_post_processing.app.util.CracUtil.importNativeCrac;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public class DailyF303Generator {

    public static final String CRAC_CREATION_PARAMETERS_JSON = "/crac/cracCreationParameters.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(DailyF303Generator.class);
    private final MinioAdapter minioAdapter;

    public DailyF303Generator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public FlowBasedConstraintDocument generate(Set<TaskDto> tasks) {
        String cracFilePath = tasks.stream()
            .findFirst().orElseThrow()
            .getInputs()
            .stream().filter(processFileDto -> processFileDto.getFileType().equals("CBCORA"))
            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException("task dto missing cbcora file"))
            .getFilePath();
        CracCreationParameters cracCreationParameters = getCimCracCreationParameters();
        FlowBasedConstraintDocument flowBasedConstraintDocument;
        try (final InputStream cracXmlInputStream = minioAdapter.getFileFromFullPath(cracFilePath)) {
            flowBasedConstraintDocument = importNativeCrac(cracXmlInputStream);
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during F303 file creation", e);
        }
        Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(flowBasedConstraintDocument.getConstraintTimeInterval().getV());
        List<HourlyF303Info> hourlyF303Infos = new ArrayList<>();
        positionMap.values().forEach(interval -> {
            Optional<TaskDto> taskDtoOptional =  getTaskDtoOfInterval(interval, tasks);
            if (taskDtoOptional.isPresent()) {
                TaskDto taskDto = taskDtoOptional.get();
                try {
                    Network network = getNetworkOfTaskDto(taskDto, minioAdapter);
                    FbConstraintCreationContext crac = getCracOfTaskDto(network, taskDto, minioAdapter, cracCreationParameters);
                    RaoResult raoResult = getRaoResultOfTaskDto(crac, taskDto, minioAdapter);
                    if (taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
                        hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForSuccessfulInterval(flowBasedConstraintDocument, interval, taskDto.getTimestamp(), crac, raoResult));
                    } else {
                        hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForNonRequestedOrFailedInterval(flowBasedConstraintDocument, interval));
                    }
                } catch (Exception e) {
                    hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForNonRequestedOrFailedInterval(flowBasedConstraintDocument, interval));
                }
            } else {
                hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForNonRequestedOrFailedInterval(flowBasedConstraintDocument, interval));
            }
        });

        // gather hourly info in one common document, cluster the elements that can be clusterized
        return new DailyF303Clusterizer(hourlyF303Infos, flowBasedConstraintDocument).generateClusterizedDocument();
    }

    private Optional<TaskDto> getTaskDtoOfInterval(Interval interval, Set<TaskDto> taskDtos) {
        return taskDtos.stream().filter(taskDto -> interval.contains(taskDto.getTimestamp().toInstant())).findFirst();
    }

    private CracCreationParameters getCimCracCreationParameters() {
        LOGGER.info("Importing Crac Creation Parameters file: {}", CRAC_CREATION_PARAMETERS_JSON);
        return JsonCracCreationParameters.read(getClass().getResourceAsStream(CRAC_CREATION_PARAMETERS_JSON));
    }

    private Network getNetworkOfTaskDto(TaskDto taskDto, MinioAdapter minioAdapter) {
        Optional<ProcessFileDto> processFileDto = taskDto.getOutputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("CGM_OUT"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new RuntimeException("Missing file");
        }
        try (InputStream networkInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return Network.read(processFileDto.get().getFilename(), networkInputStream);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import network of task %s", taskDto.getTimestamp()), e);
        }
    }

    private FbConstraintCreationContext getCracOfTaskDto(Network network, TaskDto taskDto, MinioAdapter minioAdapter, CracCreationParameters cracCreationParameters) {
        Optional<ProcessFileDto> processFileDto = taskDto.getOutputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("CBCORA"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new RuntimeException("Missing file");
        }
        try (InputStream cracInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return (FbConstraintCreationContext) new FbConstraintImporter().importData(cracInputStream, cracCreationParameters, network, taskDto.getTimestamp());
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import network of task %s", taskDto.getTimestamp()), e);
        }
    }

    private RaoResult getRaoResultOfTaskDto(FbConstraintCreationContext crac, TaskDto taskDto, MinioAdapter minioAdapter) {
        Optional<ProcessFileDto> processFileDto = taskDto.getOutputs()
                .stream()
                .filter(dto -> dto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED) &&
                        dto.getFileType().equals("RAO_RESULT"))
                .findAny();
        if (processFileDto.isEmpty()) {
            throw new RuntimeException("Missing file");
        }
        try (InputStream raoResultInputStream = minioAdapter.getFileFromFullPath(processFileDto.get().getFilePath())) {
            return RaoResult.read(raoResultInputStream, crac.getCrac());
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import RAO result of hourly RAO response of instant %s", taskDto.getTimestamp()), e);
        }
    }
}
