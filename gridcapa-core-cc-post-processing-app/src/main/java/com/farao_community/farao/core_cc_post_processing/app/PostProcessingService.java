/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.services.*;
import com.farao_community.farao.core_cc_post_processing.app.util.NamingRules;
import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.generateOverallStatus;
import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.getFirstInstant;
import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.getLastInstant;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public class PostProcessingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);
    public static final String OUTPUTS_DIR = "RAO_OUTPUTS_DIR/";
    private final MinioAdapter minioAdapter;
    private final ZipAndUploadService zipAndUploadService;
    private final RaoMetadata raoMetadata = new RaoMetadata();

    public PostProcessingService(MinioAdapter minioAdapter,
                                 ZipAndUploadService zipAndUploadService) {
        this.minioAdapter = minioAdapter;
        this.zipAndUploadService = zipAndUploadService;
    }

    public void processTasks(LocalDate localDate, Set<TaskDto> tasksToPostProcess, List<byte[]> logList) {
        String outputsTargetMinioFolder = generateTargetMinioFolder(localDate);
        // Fetch hourly outputs generated by core-cc runner
        Map<TaskDto, ProcessFileDto> cnePerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> cgmPerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> metadataPerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> raoResultPerTask = new HashMap<>();
        fillMapsOfOutputs(tasksToPostProcess, cnePerTask, cgmPerTask, metadataPerTask, raoResultPerTask);
        //get version of outputs or default to 1
        final int outputFileVersion = getOutputFileVersion(tasksToPostProcess);
        // Generate outputs
        //Rao Result files to one zip
        zipAndUploadService.zipRaoResultsAndSendToOutputs(outputsTargetMinioFolder, raoResultPerTask, localDate);
        // -- F341 : metadata file
        Map<UUID, CoreCCMetadata> metadataMap = fetchMetadataFromMinio(metadataPerTask);
        try {
            // Only write metadata for timestamps with a RaoRequestInstant defined
            zipAndUploadService.uploadF341ToMinio(outputsTargetMinioFolder,
                    CoreCCMetadataGenerator.generateMetadataCsv(new ArrayList<>(metadataMap.entrySet().stream()
                            .filter(entry -> Objects.nonNull(entry.getValue().getRaoRequestInstant()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)).values()), raoMetadata).getBytes(),
                    raoMetadata,
                    outputFileVersion);
        } catch (Exception e) {
            String errorMessage = "Could not generate metadata file for core cc : " + e.getMessage();
            LOGGER.error(errorMessage);
            throw new CoreCCPostProcessingInternalException("Could not generate metadata file", e);
        }

        // -- F342 : zipped logs
        zipAndUploadService.zipAndUploadLogs(logList, NamingRules.generateZippedLogsName(raoMetadata.getRaoRequestInstant(), outputsTargetMinioFolder, outputFileVersion));
        // -- F304 : cgms
        zipAndUploadService.zipCgmsAndSendToOutputs(outputsTargetMinioFolder, cgmPerTask, localDate, raoMetadata.getCorrelationId(), raoMetadata.getTimeInterval(), outputFileVersion);
        // -- F299 : cnes
        zipAndUploadService.zipCnesAndSendToOutputs(outputsTargetMinioFolder, cnePerTask, localDate, outputFileVersion);
        // -- F303 : flowBasedConstraintDocument
        zipAndUploadService.uploadF303ToMinio(DailyF303Generator.generate(new TaskDtoBasedDailyF303InputsProvider(tasksToPostProcess, minioAdapter)), outputsTargetMinioFolder, localDate, outputFileVersion);
        // -- F305 : RaoResponse
        zipAndUploadService.uploadF305ToMinio(outputsTargetMinioFolder, F305XmlGenerator.generateRaoResponse(tasksToPostProcess, cgmPerTask, localDate, raoMetadata.getCorrelationId(), metadataMap, raoMetadata.getTimeInterval()), localDate, outputFileVersion);
        LOGGER.info("All outputs were uploaded");
    }

    private static int getOutputFileVersion(final Set<TaskDto> tasksToPostProcess) {
        return tasksToPostProcess.stream().mapToInt(task -> task.getRunHistory().size()).max().orElse(1);
    }

    private String generateTargetMinioFolder(final LocalDate localDate) {
        return OUTPUTS_DIR + localDate;
    }

    private void fillMapsOfOutputs(Set<TaskDto> tasksToProcess,
                                   Map<TaskDto, ProcessFileDto> cnes,
                                   Map<TaskDto, ProcessFileDto> cgms,
                                   Map<TaskDto, ProcessFileDto> metadatas,
                                   Map<TaskDto, ProcessFileDto> raoResults) {
        tasksToProcess.forEach(taskDto ->
                taskDto.getOutputs()
                        .stream()
                        .filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED))
                        .forEach(processFileDto -> {
                            switch (processFileDto.getFileType()) {
                                case "CNE" -> cnes.put(taskDto, processFileDto);
                                case "CGM_OUT" -> cgms.put(taskDto, processFileDto);
                                case "METADATA" -> metadatas.put(taskDto, processFileDto);
                                case "RAO_RESULT" -> raoResults.put(taskDto, processFileDto);
                                default -> {
                                    // do nothing, other outputs are available but we won't be collecting them
                                }
                            }
                        })
        );
    }

    Map<UUID, CoreCCMetadata> fetchMetadataFromMinio(Map<TaskDto, ProcessFileDto> metadatas) {
        Map<UUID, CoreCCMetadata> metadataMap = new HashMap<>();
        metadatas
                .forEach((key, value) -> {
                    InputStream inputStream = minioAdapter.getFileFromFullPath(value.getFilePath());
                    try {
                        CoreCCMetadata coreCCMetadata = new ObjectMapper().readValue(IOUtils.toString(inputStream, StandardCharsets.UTF_8), CoreCCMetadata.class);
                        metadataMap.put(key.getId(), coreCCMetadata);
                    } catch (IOException e) {
                        throw new CoreCCPostProcessingInternalException("error while fetching individual metadata", e);
                    }
                });
        // Sanity checks
        if (metadataMap.values().stream().map(CoreCCMetadata::getTimeInterval).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong time Interval in metadata");
        }
        if (metadataMap.values().stream().map(CoreCCMetadata::getRaoRequestFileName).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong Rao request file name in metadata");
        }
        if (metadataMap.values().stream().map(CoreCCMetadata::getVersion).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong version in metadata");
        }
        if (metadataMap.values().stream().map(CoreCCMetadata::getCorrelationId).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong correlationId in metadata");
        }
        // Define raoMetadata attributes
        raoMetadata.setStatus(generateOverallStatus(metadataMap.values().stream().map(CoreCCMetadata::getStatus).collect(Collectors.toSet())));
        raoMetadata.setTimeInterval(metadataMap.values().stream().map(CoreCCMetadata::getTimeInterval).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setRequestReceivedInstant(getFirstInstant(metadataMap.values().stream().map(CoreCCMetadata::getRequestReceivedInstant).collect(Collectors.toSet())));
        raoMetadata.setRaoRequestFileName(metadataMap.values().stream().map(CoreCCMetadata::getRaoRequestFileName).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setVersion(metadataMap.values().stream().map(CoreCCMetadata::getVersion).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setCorrelationId(metadataMap.values().stream().map(CoreCCMetadata::getCorrelationId).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setOutputsSendingInstant(Instant.now().toString());
        // The following metadata can be null
        raoMetadata.setComputationStartInstant(getFirstInstant(metadataMap.values().stream().map(CoreCCMetadata::getComputationStart).filter(Objects::nonNull).collect(Collectors.toSet())));
        raoMetadata.setComputationEndInstant(getLastInstant(metadataMap.values().stream().map(CoreCCMetadata::getComputationEnd).filter(Objects::nonNull).collect(Collectors.toSet())));
        raoMetadata.setRaoRequestInstant(getLastInstant(metadataMap.values().stream().map(CoreCCMetadata::getRaoRequestInstant).filter(Objects::nonNull).collect(Collectors.toSet())));
        return metadataMap;
    }

}
