/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.Indicator.*;

/**
 * @author Peter Mitri {@literal <peter.mitri at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 */
public final class CoreCCMetadataGenerator {

    private static final String UNDEFINED_COMPUTATION_TIME = "UNDEFINED";

    private CoreCCMetadataGenerator() {
    }

    public static String generateMetadataCsv(List<CoreCCMetadata> metadataList, RaoMetadata macroMetadata) {
        MultiKeyMap data = structureDataFromTask(metadataList, macroMetadata);
        return writeCsvFromMap(data, metadataList, macroMetadata.getTimeInterval());
    }

    private static MultiKeyMap structureDataFromTask(List<CoreCCMetadata> metadataList, RaoMetadata macroMetada) {
        // Store data in a MultiKeyMap
        // First key is column (indicator)
        // Second key is timestamp (or whole business day)
        // Value is the value of the indicator for the given timestamp
        MultiKeyMap data = new MultiKeyMap();

        // Compute updated overall status : only timestamps with a RaoRequestInstant defined are considered
        macroMetada.setStatus(RaoMetadata.generateOverallStatus(metadataList.stream().map(CoreCCMetadata::getStatus).collect(Collectors.toSet())));
        final String timeInterval = macroMetada.getTimeInterval();
        data.put(RAO_REQUESTS_RECEIVED, timeInterval, macroMetada.getRaoRequestFileName());
        data.put(RAO_REQUEST_RECEPTION_TIME, timeInterval, macroMetada.getRequestReceivedInstant());
        data.put(RAO_OUTPUTS_SENT, timeInterval, "SUCCESS".equals(macroMetada.getStatus()) ? "YES" : "NO");
        data.put(RAO_OUTPUTS_SENDING_TIME, timeInterval, macroMetada.getOutputsSendingInstant());
        data.put(RAO_COMPUTATION_STATUS, timeInterval, macroMetada.getStatus());
        data.put(RAO_START_TIME, timeInterval, macroMetada.getComputationStartInstant());
        data.put(RAO_END_TIME, timeInterval, macroMetada.getComputationEndInstant());
        data.put(RAO_COMPUTATION_TIME, timeInterval, getComputationTime(macroMetada.getComputationStartInstant(), macroMetada.getComputationEndInstant()));
        metadataList.forEach(individualMetadata -> {
            final String raoRequestInstant = individualMetadata.getRaoRequestInstant();
            data.put(RAO_START_TIME, raoRequestInstant, individualMetadata.getComputationStart());
            data.put(RAO_END_TIME, raoRequestInstant, individualMetadata.getComputationEnd());
            data.put(RAO_COMPUTATION_TIME, raoRequestInstant, getComputationTime(individualMetadata.getComputationStart(), individualMetadata.getComputationEnd()));
            data.put(RAO_RESULTS_PROVIDED, raoRequestInstant, individualMetadata.getStatus().equals("SUCCESS") ? "YES" : "NO");
            data.put(RAO_COMPUTATION_STATUS, raoRequestInstant, individualMetadata.getStatus());
        });
        return data;
    }

    private static String writeCsvFromMap(MultiKeyMap data, List<CoreCCMetadata> metadataList, String timeInterval) {
        // Get headers for columns & lines
        List<RaoMetadata.Indicator> indicators = Arrays.stream(values())
                .sorted(Comparator.comparing(RaoMetadata.Indicator::getOrder))
                .toList();
        List<String> timestamps = metadataList.stream().map(CoreCCMetadata::getRaoRequestInstant).sorted(String::compareTo).collect(Collectors.toList());
        timestamps.add(0, timeInterval);

        // Generate CSV string
        char delimiter = ';';
        char cr = '\n';
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append(delimiter);
        csvBuilder.append(indicators.stream().map(RaoMetadata.Indicator::getCsvLabel).collect(Collectors.joining(";")));
        csvBuilder.append(cr);
        for (String timestamp : timestamps) {
            csvBuilder.append(timestamp);
            for (RaoMetadata.Indicator indicator : indicators) {
                String value = data.containsKey(indicator, timestamp) && data.get(indicator, timestamp) != null ? data.get(indicator, timestamp).toString() : "";
                csvBuilder.append(delimiter);
                csvBuilder.append(value);
            }
            csvBuilder.append(cr);
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
