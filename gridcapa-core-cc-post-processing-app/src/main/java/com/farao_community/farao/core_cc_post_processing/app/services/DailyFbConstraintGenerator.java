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
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.powsybl.openrao.virtualhubs.InternalHvdc;
import com.powsybl.openrao.virtualhubs.VirtualHubsConfiguration;
import com.powsybl.openrao.virtualhubs.xml.XmlVirtualHubsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
public class DailyFbConstraintGenerator {

    public static final String CRAC_CREATION_PARAMETERS_JSON = "/crac/cracCreationParameters.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(DailyFbConstraintGenerator.class);
    private final MinioAdapter minioAdapter;

    public DailyFbConstraintGenerator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public FlowBasedConstraintDocument generate(final Map<TaskDto, ProcessFileDto> raoResults,
                                                final Map<TaskDto, ProcessFileDto> cgms) {
        final List<ProcessFileDto> inputs = raoResults.keySet().stream()
            .findFirst().orElseThrow()
            .getInputs();
        final String virtualHubsFilePath = getFilePath(inputs, "VIRTUALHUB");
        final String cracFilePath = getFilePath(inputs, "CBCORA");

        final CracCreationParameters cracCreationParameters = getCimCracCreationParameters();
        try (
            final InputStream virtualHubsInputStream = minioAdapter.getFileFromFullPath(virtualHubsFilePath);
            final InputStream cracXmlInputStream = minioAdapter.getFileFromFullPath(cracFilePath)
        ) {
            final VirtualHubsConfiguration virtualHubsConfiguration = XmlVirtualHubsConfiguration.importConfiguration(virtualHubsInputStream);
            final List<InternalHvdc> internalHvdcs = virtualHubsConfiguration.getInternalHvdcs();

            final byte[] cracXmlBytes = getBytesFromInputStream(cracXmlInputStream);
            final FlowBasedConstraintDocument flowBasedConstraintDocument;
            try (final InputStream nativeCracInputStream = new ByteArrayInputStream(cracXmlBytes)) {
                flowBasedConstraintDocument = importNativeCrac(nativeCracInputStream);
            }

            // generate FbConstraintInfo for each hour of the initial CRAC
            final Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(flowBasedConstraintDocument.getConstraintTimeInterval().getV());
            final List<HourlyFbConstraintInfo> hourlyFbConstraintInfos = new ArrayList<>();

            positionMap.values().forEach(interval ->
                addHourlyFbConstraintInfo(
                    raoResults,
                    cgms,
                    interval,
                    cracXmlBytes,
                    flowBasedConstraintDocument,
                    cracCreationParameters,
                    internalHvdcs,
                    hourlyFbConstraintInfos
                ));

            // gather hourly info in one common document, cluster the elements that can be clusterized
            return new DailyFbConstraintClusterizer(hourlyFbConstraintInfos, flowBasedConstraintDocument).generateClusterizedDocument();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during CBCORA file creation", e);
        }
    }

    private static String getFilePath(final List<ProcessFileDto> inputs, final String filetype) {
        return inputs.stream()
            .filter(processFileDto -> processFileDto.getFileType().equals(filetype))
            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException(String.format("Task dto missing %s file", filetype)))
            .getFilePath();
    }

    private void addHourlyFbConstraintInfo(final Map<TaskDto, ProcessFileDto> raoResults,
                                           final Map<TaskDto, ProcessFileDto> cgms,
                                           final Interval interval,
                                           final byte[] cracXmlBytes,
                                           final FlowBasedConstraintDocument flowBasedConstraintDocument,
                                           final CracCreationParameters cracCreationParameters,
                                           final List<InternalHvdc> internalHvdcs,
                                           final List<HourlyFbConstraintInfo> hourlyFbConstraintInfos) {
        final Optional<TaskDto> taskDtoOptional = getTaskDtoOfInterval(interval, raoResults.keySet());
        if (taskDtoOptional.isPresent()) {
            final TaskDto taskDto = taskDtoOptional.get();
            try (final InputStream cracXmlInputStream = new ByteArrayInputStream(cracXmlBytes)) {
                hourlyFbConstraintInfos.add(
                    new HourlyFbConstraintInfoGenerator(flowBasedConstraintDocument, interval, taskDto, minioAdapter, cracCreationParameters, internalHvdcs)
                        .generate(raoResults.get(taskDto), cgms.get(taskDto), cracXmlInputStream)
                );
            } catch (final IOException e) {
                throw new CoreCCPostProcessingInternalException(
                    String.format("Exception occurred while reading hourly data for timestamp %s", taskDto.getTimestamp()),
                    e
                );
            }
        } else {
            LOGGER.warn("Cannot find taskDto for interval {}", interval);
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
