/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.powsybl.openrao.data.cracapi.parameters.CracCreationParameters;
import com.powsybl.openrao.data.cracapi.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.craccreation.creator.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.farao_community.farao.core_cc_post_processing.app.util.CracUtil.getBytesFromInputStream;
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

    public FlowBasedConstraintDocument generate(Map<TaskDto, ProcessFileDto> raoResults, Map<TaskDto, ProcessFileDto> cgms) {
        ProcessFileDto cracFile = raoResults.keySet().stream()
            .findFirst().orElseThrow()
            .getInputs()
            .stream().filter(processFileDto -> processFileDto.getFileType().equals("CBCORA"))
            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException("task dto missing cbcora file"));
        CracCreationParameters cracCreationParameters = getCimCracCreationParameters();
        try (final InputStream cracXmlInputStream = minioAdapter.getFileFromFullPath(cracFile.getFilePath())) {
            final byte[] cracXmlBytes = getBytesFromInputStream(cracXmlInputStream);
            final FlowBasedConstraintDocument flowBasedConstraintDocument;
            try (final InputStream firstUseStream = new ByteArrayInputStream(cracXmlBytes)) {
                flowBasedConstraintDocument = importNativeCrac(firstUseStream);
            }
            // generate F303Info for each hour of the initial CRAC
            Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(flowBasedConstraintDocument.getConstraintTimeInterval().getV());
            List<HourlyF303Info> hourlyF303Infos = new ArrayList<>();
            positionMap.values().forEach(interval -> {
                Optional<TaskDto> taskDtoOptional =  getTaskDtoOfInterval(interval, raoResults.keySet());
                if (taskDtoOptional.isPresent()) {
                    TaskDto taskDto = taskDtoOptional.get();
                    try (final InputStream tempCracXmlInputStream = new ByteArrayInputStream(cracXmlBytes)) {
                        hourlyF303Infos.add(new HourlyF303InfoGenerator(flowBasedConstraintDocument, interval, taskDto, minioAdapter, cracCreationParameters)
                                .generate(raoResults.get(taskDto), cgms.get(taskDto), tempCracXmlInputStream));
                    } catch (final IOException e) {
                        throw new CoreCCPostProcessingInternalException("Exception occurred during F303 file creation", e);
                    }
                } else {
                    LOGGER.warn(String.format("Cannot find taskDto for interval %s", interval));
                }
            });

            // gather hourly info in one common document, cluster the elements that can be clusterized
            return new DailyF303Clusterizer(hourlyF303Infos, flowBasedConstraintDocument).generateClusterizedDocument();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during F303 file creation", e);
        }
    }

    private Optional<TaskDto> getTaskDtoOfInterval(Interval interval, Set<TaskDto> taskDtos) {
        return taskDtos.stream().filter(taskDto -> interval.contains(taskDto.getTimestamp().toInstant())).findFirst();
    }

    private CracCreationParameters getCimCracCreationParameters() {
        LOGGER.info("Importing Crac Creation Parameters file: {}", CRAC_CREATION_PARAMETERS_JSON);
        return JsonCracCreationParameters.read(getClass().getResourceAsStream(CRAC_CREATION_PARAMETERS_JSON));
    }
}
