/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.collections.map.MultiKeyMap;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
public class CoreCCMetadataGenerator {

    private final MinioAdapter minioAdapter;

    public CoreCCMetadataGenerator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public static final ZoneId ZONE_ID = ZoneId.of("Europe/Brussels");
    public static final String OUTPUTS = "%s/outputs/%s"; // destination/filename
    public static final DateTimeFormatter METADATA_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("'CASTOR-RAO_22VCOR0CORE0PRDI_RTE-F341_'yyyyMMdd'-F341-0V.csv'").withZone(ZONE_ID);

    public void exportMetadataFile(String targetMinioFolder, List<CoreCCMetadata> metadataList, RaoMetadata macroMetadata) {
        byte[] csv = generateMetadataCsv(metadataList, macroMetadata).getBytes();
        String metadataFileName = generateMetadataFileName(macroMetadata.getRaoRequestInstant(), macroMetadata.getVersion());
        String metadataDestinationPath = generateOutputsDestinationPath(targetMinioFolder, metadataFileName);

        try (InputStream csvIs = new ByteArrayInputStream(csv)) {
            minioAdapter.uploadOutput(metadataDestinationPath, csvIs);
        } catch (IOException e) {
            throw new CoreCCInternalException("Exception occurred while uploading metadata file");
        }
    }

    public static String generateMetadataFileName(String instant, int version) {
        return METADATA_FILENAME_FORMATTER.format(Instant.parse(instant))
            .replace("0V", String.format("%02d", version));
    }

    public static String generateOutputsDestinationPath(String destinationPrefix, String fileName) {
        return String.format(OUTPUTS, destinationPrefix, fileName);
    }

    private static String generateMetadataCsv(List<CoreCCMetadata> metadataList, RaoMetadata macroMetadata) {
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

        data.put(RAO_REQUESTS_RECEIVED, macroMetada.getTimeInterval(), macroMetada.getRaoRequestFileName());
        data.put(RAO_REQUEST_RECEPTION_TIME, macroMetada.getTimeInterval(), macroMetada.getRequestReceivedInstant());
        data.put(RAO_OUTPUTS_SENT, macroMetada.getTimeInterval(), macroMetada.getStatus().equals("SUCCESS") ? "YES" : "NO");
        data.put(RAO_OUTPUTS_SENDING_TIME, macroMetada.getTimeInterval(), macroMetada.getOutputsSendingInstant());
        data.put(RAO_COMPUTATION_STATUS, macroMetada.getTimeInterval(), macroMetada.getStatus());
        data.put(RAO_START_TIME, macroMetada.getTimeInterval(), macroMetada.getComputationStartInstant());
        data.put(RAO_END_TIME, macroMetada.getTimeInterval(), macroMetada.getComputationEndInstant());
        data.put(RAO_COMPUTATION_TIME, macroMetada.getTimeInterval(), String.valueOf(ChronoUnit.MINUTES.between(Instant.parse(macroMetada.getComputationStartInstant()), Instant.parse(macroMetada.getComputationEndInstant()))));
        metadataList.forEach(individualMetadata -> {
            data.put(RAO_START_TIME, individualMetadata.getRaoRequestInstant(), individualMetadata.getComputationStart());
            data.put(RAO_END_TIME, individualMetadata.getRaoRequestInstant(), individualMetadata.getComputationEnd());
            data.put(RAO_COMPUTATION_TIME, individualMetadata.getRaoRequestInstant(), String.valueOf(ChronoUnit.MINUTES.between(Instant.parse(individualMetadata.getComputationStart()), Instant.parse(individualMetadata.getComputationEnd()))));
            data.put(RAO_RESULTS_PROVIDED, individualMetadata.getRaoRequestInstant(), individualMetadata.getStatus().equals("SUCCESS") ? "YES" : "NO");
            data.put(RAO_COMPUTATION_STATUS, individualMetadata.getRaoRequestInstant(), individualMetadata.getStatus());
        });
        return data;
    }

    private static String writeCsvFromMap(MultiKeyMap data, List<CoreCCMetadata> metadataList, String timeInterval) {
        // Get headers for columns & lines
        List<RaoMetadata.Indicator> indicators = Arrays.stream(values())
                .sorted(Comparator.comparing(RaoMetadata.Indicator::getOrder))
                .collect(Collectors.toList());
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
                String value = data.containsKey(indicator, timestamp) ? data.get(indicator, timestamp).toString() : "";
                csvBuilder.append(delimiter);
                csvBuilder.append(value);
            }
            csvBuilder.append(cr);
        }
        return csvBuilder.toString();
    }
}
