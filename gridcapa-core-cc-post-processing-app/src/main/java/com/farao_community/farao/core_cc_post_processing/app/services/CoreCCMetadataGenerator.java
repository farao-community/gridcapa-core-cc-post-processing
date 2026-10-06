/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.entities.ComputationArea;
import com.farao_community.farao.core_cc_post_processing.app.entities.DailyMetadata;
import com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator;
import com.farao_community.farao.core_cc_post_processing.app.util.MetadataUtil;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.CONTINENTAL_RAO_COMPUTATION_STATUS;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.CONTINENTAL_RAO_COMPUTATION_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.CONTINENTAL_RAO_END_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.CONTINENTAL_RAO_START_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_COMPUTATION_STATUS;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_OUTPUTS_SENDING_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_OUTPUTS_SENT;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_REQUESTS_RECEIVED;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_REQUEST_RECEPTION_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.RAO_RESULTS_PROVIDED;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.SEM_RAO_COMPUTATION_STATUS;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.SEM_RAO_COMPUTATION_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.SEM_RAO_END_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataIndicator.SEM_RAO_START_TIME;
import static com.farao_community.farao.core_cc_post_processing.app.util.MetadataUtil.getResultsProvidedStatus;

/**
 * @author Peter Mitri {@literal <peter.mitri at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 */
public final class CoreCCMetadataGenerator {

    private static final char CSV_DELIMITER = ';';
    private static final char CSV_CR = '\n';
    private static final String UNDEFINED_COMPUTATION_TIME = "UNDEFINED";

    private CoreCCMetadataGenerator() {
    }

    public static String generateMetadataCsv(final List<CoreCCMetadata> hourlyMetadataList, final DailyMetadata dailyMetadata) {
        final MultiKeyMap<Object, String> data = structureDataFromTask(hourlyMetadataList, dailyMetadata);
        return writeCsvFromMap(data, hourlyMetadataList, dailyMetadata.getTimeInterval());
    }

    private static MultiKeyMap<Object, String> structureDataFromTask(final List<CoreCCMetadata> hourlyMetadataList, final DailyMetadata dailyMetadata) {
        // Store data in a MultiKeyMap
        // First key is column (indicator)
        // Second key is timestamp (or whole business day)
        // Value is the value of the indicator for the given timestamp
        final MultiKeyMap<Object, String> data = new MultiKeyMap<>();

        // Recompute the overall status using only the timestamps that have a non-null RaoRequestInstant and update the dailyMetadata object
        dailyMetadata.setStatus(MetadataUtil.generateOverallStatus(hourlyMetadataList, ComputationArea.ALL));

        // Daily data
        final String timeInterval = dailyMetadata.getTimeInterval();
        data.put(RAO_REQUESTS_RECEIVED, timeInterval, dailyMetadata.getRaoRequestFileName());
        data.put(RAO_REQUEST_RECEPTION_TIME, timeInterval, dailyMetadata.getRequestReceivedInstant());
        data.put(RAO_OUTPUTS_SENT, timeInterval, getResultsProvidedStatus(dailyMetadata.getStatus()));
        data.put(RAO_OUTPUTS_SENDING_TIME, timeInterval, dailyMetadata.getOutputsSendingInstant());
        data.put(RAO_COMPUTATION_STATUS, timeInterval, dailyMetadata.getStatus());
        putAreaRelatedData(
            data, timeInterval,
            dailyMetadata.getSemComputationStatus(), dailyMetadata.getSemComputationStart(), dailyMetadata.getSemComputationEnd(),
            dailyMetadata.getContinentalComputationStatus(), dailyMetadata.getContinentalComputationStart(), dailyMetadata.getContinentalComputationEnd()
        );

        // Hourly data
        hourlyMetadataList.forEach(individualMetadata -> {
            final String raoRequestInstant = individualMetadata.getRaoRequestInstant();
            data.put(RAO_RESULTS_PROVIDED, raoRequestInstant, getResultsProvidedStatus(individualMetadata.getContinentalComputationStatus(), individualMetadata.getSemComputationStatus()));
            putAreaRelatedData(
                data, raoRequestInstant,
                individualMetadata.getSemComputationStatus(), individualMetadata.getSemComputationStart(), individualMetadata.getSemComputationEnd(),
                individualMetadata.getContinentalComputationStatus(), individualMetadata.getContinentalComputationStart(), individualMetadata.getContinentalComputationEnd()
            );
        });
        return data;
    }

    private static void putAreaRelatedData(final MultiKeyMap<Object, String> data,
                                           final String timeInterval,
                                           final String semComputationStatus,
                                           final String semComputationStart,
                                           final String semComputationEnd,
                                           final String continentalComputationStatus,
                                           final String continentalComputationStart,
                                           final String continentalComputationEnd) {
        data.put(SEM_RAO_COMPUTATION_STATUS, timeInterval, semComputationStatus);
        data.put(SEM_RAO_START_TIME, timeInterval, semComputationStart);
        data.put(SEM_RAO_END_TIME, timeInterval, semComputationEnd);
        final String semComputationTime = semComputationStatus == null && semComputationStart == null && semComputationEnd == null
            ? ""
            : getComputationTime(semComputationStart, semComputationEnd);
        data.put(SEM_RAO_COMPUTATION_TIME, timeInterval, semComputationTime);
        data.put(CONTINENTAL_RAO_COMPUTATION_STATUS, timeInterval, continentalComputationStatus);
        data.put(CONTINENTAL_RAO_START_TIME, timeInterval, continentalComputationStart);
        data.put(CONTINENTAL_RAO_END_TIME, timeInterval, continentalComputationEnd);
        final String continentalComputationTime = continentalComputationStatus == null && continentalComputationStart == null && continentalComputationEnd == null
            ? ""
            : getComputationTime(continentalComputationStart, continentalComputationEnd);
        data.put(CONTINENTAL_RAO_COMPUTATION_TIME, timeInterval, continentalComputationTime);
    }

    private static String writeCsvFromMap(final MultiKeyMap<Object, String> data,
                                          final List<CoreCCMetadata> metadataList,
                                          final String timeInterval) {
        // Get headers for columns & lines
        final List<MetadataIndicator> indicators = Arrays.stream(MetadataIndicator.values())
            .sorted(Comparator.comparing(MetadataIndicator::getOrder))
            .toList();
        final List<String> timestamps = metadataList.stream().map(CoreCCMetadata::getRaoRequestInstant).sorted(String::compareTo).collect(Collectors.toList()); // NOSONAR because the resulting list should be modifiable
        timestamps.addFirst(timeInterval);

        // Generate CSV string
        final StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append(CSV_DELIMITER);
        csvBuilder.append(indicators.stream().map(MetadataIndicator::getCsvLabel).collect(Collectors.joining(";")));
        csvBuilder.append(CSV_CR);
        for (final String timestamp : timestamps) {
            csvBuilder.append(timestamp);
            for (final MetadataIndicator indicator : indicators) {
                final String value = data.get(indicator, timestamp) != null ? data.get(indicator, timestamp) : "";
                csvBuilder.append(CSV_DELIMITER);
                csvBuilder.append(value);
            }
            csvBuilder.append(CSV_CR);
        }
        return csvBuilder.toString();
    }

    private static String getComputationTime(String startTime, String endTime) {
        if (StringUtils.isBlank(startTime) || StringUtils.isBlank(endTime)) {
            return UNDEFINED_COMPUTATION_TIME;
        } else {
            return String.valueOf(ChronoUnit.MINUTES.between(Instant.parse(startTime), Instant.parse(endTime)));
        }
    }
}
