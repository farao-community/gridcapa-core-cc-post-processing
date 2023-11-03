/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.core_cc_post_processing.app.services.*;
import com.farao_community.farao.core_cc_post_processing.app.util.*;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.*;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public class PostProcessingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);
    public static final String CORE_CC = "CORE/CC/";
    public static final String OUTPUTS_DIR = "RAO_OUTPUTS_DIR/";
    private final MinioAdapter minioAdapter;
    private RaoMetadata raoMetadata = new RaoMetadata();

    public PostProcessingService(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public void processTasks(LocalDate localDate, Set<TaskDto> tasksToPostProcess, List<byte[]> logList) {
        String outputsTargetMinioFolder = generateTargetMinioFolder(localDate);
        // Fetch hourly outputs generated by core-cc runner
        Map<TaskDto, ProcessFileDto> cnePerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> cgmPerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> metadataPerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> raoResultPerTask = new HashMap<>();
        fillMapsOfOutputs(tasksToPostProcess, cnePerTask, cgmPerTask, metadataPerTask, raoResultPerTask);

        // Generate outputs
        // -- F341 : metadata file
        Map<UUID, CoreCCMetadata> metadataMap = fetchMetadataFromMinio(metadataPerTask);
        try {
            // Only write metadata for timestamps with a RaoRequestInstant defined
            uploadF341ToMinio(outputsTargetMinioFolder,
                CoreCCMetadataGenerator.generateMetadataCsv(metadataMap.entrySet().stream()
                    .filter(entry -> Objects.nonNull(entry.getValue().getRaoRequestInstant()))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())).values()
                    .stream().collect(Collectors.toList()), raoMetadata).getBytes());
        } catch (Exception e) {
            LOGGER.error("Could not generate metadata file for core cc : {}", e.getMessage());
            throw new CoreCCPostProcessingInternalException("Could not generate metadata file");
        }

        // -- F342 : zipped logs
        zipAndUploadLogs(logList, NamingRules.generateZippedLogsName(raoMetadata.getRaoRequestInstant(), outputsTargetMinioFolder, raoMetadata.getVersion()));

        // -- F304 : cgms
        zipCgmsAndSendToOutputs(outputsTargetMinioFolder, cgmPerTask, localDate, raoMetadata.getCorrelationId(), raoMetadata.getTimeInterval());
        // -- F299 : cnes
        zipCnesAndSendToOutputs(outputsTargetMinioFolder, cnePerTask, localDate);
        // -- F303 : flowBasedConstraintDocument
        uploadF303ToMinio(new DailyF303Generator(minioAdapter).generate(raoResultPerTask, cgmPerTask), outputsTargetMinioFolder, localDate);
        // -- F305 : RaoResponse
        uploadF305ToMinio(outputsTargetMinioFolder, F305XmlGenerator.generateRaoResponse(tasksToPostProcess, localDate, raoMetadata.getCorrelationId(), metadataMap, raoMetadata.getTimeInterval()), localDate);
        LOGGER.info("All outputs were uploaded");
    }

    private String generateTargetMinioFolder(LocalDate localDate) {
        return OUTPUTS_DIR + localDate;
    }

    private void fillMapsOfOutputs(Set<TaskDto> tasksToProcess,
                                   Map<TaskDto, ProcessFileDto> cnes,
                                   Map<TaskDto, ProcessFileDto> cgms,
                                   Map<TaskDto, ProcessFileDto> metadatas,
                                   Map<TaskDto, ProcessFileDto> raoResults) {
        tasksToProcess.forEach(taskDto ->
            taskDto.getOutputs().forEach(processFileDto -> {
                switch (processFileDto.getFileType()) {
                    case "CNE":
                        cnes.put(taskDto, processFileDto);
                        break;
                    case "CGM_OUT":
                        cgms.put(taskDto, processFileDto);
                        break;
                    case "METADATA":
                        metadatas.put(taskDto, processFileDto);
                        break;
                    case "RAO_RESULT":
                        raoResults.put(taskDto, processFileDto);
                        break;
                    default:
                        // do nothing, other outputs are available but we won't be collecting them
                }
            })
        );
    }

    private Map<UUID, CoreCCMetadata> fetchMetadataFromMinio(Map<TaskDto, ProcessFileDto> metadatas) {
        Map<UUID, CoreCCMetadata> metadataMap = new HashMap<>();
        metadatas.entrySet().stream().filter(md -> md.getValue().getProcessFileStatus().equals(ProcessFileStatus.VALIDATED)).
            forEach(metadata -> {
                InputStream inputStream = minioAdapter.getFileFromFullPath(metadata.getValue().getFilePath());
                try {
                    CoreCCMetadata coreCCMetadata = new ObjectMapper().readValue(IOUtils.toString(inputStream, StandardCharsets.UTF_8), CoreCCMetadata.class);
                    metadataMap.put(metadata.getKey().getId(), coreCCMetadata);
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

    // -------------------- MINIO UPLOADS ---------------

    private void zipAndUploadLogs(List<byte[]> logList, String logFileName) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (byte[] bytes : logList) {
                ZipUtil.collectAndZip(zos, bytes);
            }
            zos.close();
            // upload zipped result
            try (InputStream logZipIs = new ByteArrayInputStream(baos.toByteArray())) {
                minioAdapter.uploadOutput(logFileName, logZipIs);
            } catch (IOException e) {
                throw new CoreCCPostProcessingInternalException("Error while unzipping logs", e);
            }
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException("Error while unzipping logs", e);
        }
    }

    private void zipCgmsAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cgms, LocalDate localDate, String correlationId, String timeInterval) {
        String cgmZipTmpDir = "/tmp/cgms_out/" + localDate.toString() + "/";
        // add cgm xml header to tmp folder
        F305XmlGenerator.generateCgmXmlHeaderFile(cgms.keySet(), cgmZipTmpDir, localDate, correlationId, timeInterval);

        // Add all cgms from minio to tmp folder
        cgms.values().stream().filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED)).forEach(cgm -> {
            InputStream inputStream = minioAdapter.getFileFromFullPath(cgm.getFilePath());
            File cgmFile = new File(cgmZipTmpDir + cgm.getFilename());
            try {
                FileUtils.copyInputStreamToFile(inputStream, cgmFile);
            } catch (IOException e) {
                throw new CoreCCPostProcessingInternalException("error while copying cgm to tmp folder", e);
            }
        });

        // Zip tmp folder
        byte[] cgmsZipResult = ZipUtil.zipDirectory(cgmZipTmpDir);
        String targetCgmsFolderName = NamingRules.generateCgmZipName(localDate);
        String targetCgmsFolderPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, targetCgmsFolderName);

        try (InputStream cgmZipIs = new ByteArrayInputStream(cgmsZipResult)) {
            minioAdapter.uploadOutput(targetCgmsFolderPath, cgmZipIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while zipping CGMs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(cgmZipTmpDir)); //NOSONAR
        }

    }

    private void zipCnesAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cnes, LocalDate localDate) {
        String cneZipTmpDir = "/tmp/cnes_out/" + localDate.toString() + "/";

        // Add all cnes from minio to tmp folder
        cnes.values().stream()
            .filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED))
            .forEach(cne -> {
                InputStream inputStream = minioAdapter.getFileFromFullPath(cne.getFilePath());
                File cneFile = new File(cneZipTmpDir + cne.getFilename());
                try {
                    FileUtils.copyInputStreamToFile(inputStream, cneFile);
                } catch (IOException e) {
                    throw new CoreCCPostProcessingInternalException("error while copying cne to tmp folder", e);
                }
            });

        byte[] cneZipResult = ZipUtil.zipDirectory(cneZipTmpDir);
        String targetCneFolderName = NamingRules.generateCneZipName(localDate);
        String targetCneFolderPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, targetCneFolderName);

        try (InputStream cneZipIs = new ByteArrayInputStream(cneZipResult)) {
            minioAdapter.uploadOutput(targetCneFolderPath, cneZipIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while zipping CNEs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(cneZipTmpDir)); //NOSONAR
        }
    }

    void uploadF303ToMinio(FlowBasedConstraintDocument dailyFbDocument, String targetMinioFolder, LocalDate localDate) {
        byte[] dailyFbConstraint = JaxbUtil.writeInBytes(FlowBasedConstraintDocument.class, dailyFbDocument);
        String fbConstraintFileName = NamingRules.generateOptimizedCbFileName(localDate);
        String fbConstraintDestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, fbConstraintFileName);

        try (InputStream dailyFbIs = new ByteArrayInputStream(dailyFbConstraint)) {
            minioAdapter.uploadOutput(fbConstraintDestinationPath, dailyFbIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while uploading F303 file of business day %s", localDate));
        }
    }

    void uploadF341ToMinio(String targetMinioFolder, byte[] csv) {
        String metadataFileName = NamingRules.generateMetadataFileName(raoMetadata.getRaoRequestInstant(), raoMetadata.getVersion());
        String metadataDestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, metadataFileName);
        try (InputStream csvIs = new ByteArrayInputStream(csv)) {
            minioAdapter.uploadOutput(metadataDestinationPath, csvIs);
        } catch (IOException e) {
            throw new CoreCCInternalException("Exception occurred while uploading metadata file");
        }
    }

    private void uploadF305ToMinio(String targetMinioFolder, ResponseMessageType responseMessage, LocalDate localDate) {
        byte[] responseMessageBytes = JaxbUtil.marshallMessageAndSetJaxbProperties(responseMessage);
        String f305FileName = NamingRules.generateRF305FileName(localDate);
        String f305DestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, f305FileName);

        try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
            minioAdapter.uploadOutput(f305DestinationPath, raoResponseIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while uploading F305 for business date %s", localDate));
        }
    }
}
