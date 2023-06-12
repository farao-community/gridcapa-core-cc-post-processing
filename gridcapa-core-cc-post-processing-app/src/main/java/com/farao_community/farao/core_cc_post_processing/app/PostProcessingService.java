package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.services.CoreCCMetadataGenerator;
import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.core_cc_post_processing.app.services.FileExporterHelper;
import com.farao_community.farao.core_cc_post_processing.app.services.RaoIXmlResponseGenerator;
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.OutputFileNameUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.core_cc_post_processing.app.util.ZipUtil;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata.*;

@Service
public class PostProcessingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);

    private final MinioAdapter minioAdapter;
    private final RaoIXmlResponseGenerator raoIXmlResponseGenerator;
    private final DailyF303Generator dailyF303Generator;
    private final FileExporterHelper fileExporterHelper;
    private RaoMetadata raoMetadata = new RaoMetadata();
    private List<CoreCCMetadata> metadataList = new ArrayList<>();

    public PostProcessingService(MinioAdapter minioAdapter, RaoIXmlResponseGenerator raoIXmlResponseGenerator, DailyF303Generator dailyF303Generator, FileExporterHelper fileExporterHelper) {
        this.minioAdapter = minioAdapter;
        this.raoIXmlResponseGenerator = raoIXmlResponseGenerator;
        this.dailyF303Generator = dailyF303Generator;
        this.fileExporterHelper = fileExporterHelper;
    }

    public void processTasks(LocalDate localDate, Set<TaskDto> tasksToPostProcess) {
        String outputsTargetMinioFolder = generateTargetMinioFolder(localDate);
        Map<TaskDto, ProcessFileDto> cnePerTask = new HashMap<>(); // ProcessFIleDto : donne le nom du fichier, le chemin du fichier etc ... permet de verifier le type de fichier et d'aller le recuperer sur minio
        Map<TaskDto, ProcessFileDto> cgmPerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> metadataPerTask = new HashMap<>();

        Map<TaskDto, ProcessFileDto> logPerTask = new HashMap<>();

        fillMapsOfOutputs(tasksToPostProcess, cnePerTask, cgmPerTask, metadataPerTask);

        // Generate metadata file
        fetchMetadataFromMinio(metadataPerTask);
        try {
            new CoreCCMetadataGenerator(minioAdapter).exportMetadataFile(outputsTargetMinioFolder, metadataList, raoMetadata);
        } catch (Exception e) {
            LOGGER.error("Could not generate metadata file for core cc : {}", e.getMessage());
            throw new CoreCCPostProcessingInternalException("Could not generate metadata file");
        }

        // TODO : read and save 1 ACK in outputs

        zipCgmsAndSendToOutputs(outputsTargetMinioFolder, cgmPerTask, localDate);
        zipCnesAndSendToOutputs(outputsTargetMinioFolder, cnePerTask, localDate);
        //zipLogsAndSendToOutputs(outputsTargetMinioFolder, logPerTask, localDate);

        //TODO: zip logs
        FlowBasedConstraintDocument dailyFlowBasedConstraintDocument = dailyF303Generator.generate(tasksToPostProcess);
        uploadDailyOutputFlowBasedConstraintDocument(dailyFlowBasedConstraintDocument, outputsTargetMinioFolder, localDate);
        //TODO: get correlation id
        raoIXmlResponseGenerator.generateRaoResponse(tasksToPostProcess, outputsTargetMinioFolder, localDate, "correlationId", metadataPerTask); //f305 rao response
    }

    private String generateTargetMinioFolder(LocalDate localDate) {
        return "RAO_OUTPUT_DIR/" + localDate;
    }

    private void fillMapsOfOutputs(Set<TaskDto> tasksToProcess,
                                   Map<TaskDto, ProcessFileDto> cnes,
                                   Map<TaskDto, ProcessFileDto> cgms,
                                   Map<TaskDto, ProcessFileDto> metadatas) {
        tasksToProcess.forEach(taskDto ->
            taskDto.getOutputs().forEach(processFileDto -> {
                if (taskDto.getTimestamp().toString().equals("2023-02-02T23:30Z")) {
                    System.out.println(processFileDto.getFileType());
                    System.out.println(processFileDto.getFilename());
                    System.out.println(processFileDto.getFilePath());
                }
                switch (processFileDto.getFileType()) {
                    case "CNE":
                        cnes.put(taskDto, processFileDto);
                        break;
                    case "CGM_OUT":
                        cgms.put(taskDto, processFileDto);
                        break;
                    case "METADATA":
                        metadatas.put(taskDto, processFileDto);
                }
            })
        );
    }

    private void fetchMetadataFromMinio(Map<TaskDto, ProcessFileDto> metadatas) {
        metadatas.values().stream().filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED)).
            forEach(metadata -> {
                // TODO : more robust way of fetching un-presigned url
                InputStream inputStream = minioAdapter.getFile(metadata.getFilePath().split("CORE/CC/")[1]);
                try {
                    CoreCCMetadata coreCCMetadata = new ObjectMapper().readValue(IOUtils.toString(inputStream, StandardCharsets.UTF_8), CoreCCMetadata.class);
                    metadataList.add(coreCCMetadata);
                } catch (IOException e) {
                    throw new CoreCCPostProcessingInternalException("error while fetching individual metadata", e);
                }
            });
        // Sanity checks
        if (metadataList.stream().map(CoreCCMetadata::getTimeInterval).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong time Interval in metadata");
        }
        if (metadataList.stream().map(CoreCCMetadata::getRaoRequestFileName).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong Rao request file name in metadata");
        }
        if (metadataList.stream().map(CoreCCMetadata::getVersion).collect(Collectors.toSet()).size() > 1) {
            throw new CoreCCPostProcessingInternalException("Wrong version in metadata");
        }
        // Define raoMetadata attributes
        raoMetadata.setStatus(generateOverallStatus(metadataList.stream().map(CoreCCMetadata::getStatus).collect(Collectors.toSet())));
        raoMetadata.setTimeInterval(metadataList.stream().map(CoreCCMetadata::getTimeInterval).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setRequestReceivedInstant(getFirstInstant(metadataList.stream().map(CoreCCMetadata::getRequestReceivedInstant).collect(Collectors.toSet())));
        raoMetadata.setComputationStartInstant(getFirstInstant(metadataList.stream().map(CoreCCMetadata::getComputationStart).collect(Collectors.toSet())));
        raoMetadata.setComputationEndInstant(getLastInstant(metadataList.stream().map(CoreCCMetadata::getComputationEnd).collect(Collectors.toSet())));
        raoMetadata.setOutputsSendingInstant(Instant.now().toString());
        raoMetadata.setRaoRequestFileName(metadataList.stream().map(CoreCCMetadata::getRaoRequestFileName).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setVersion(metadataList.stream().map(CoreCCMetadata::getVersion).collect(Collectors.toSet()).iterator().next());
        raoMetadata.setInstant(getLastInstant(metadataList.stream().map(CoreCCMetadata::getInstant).collect(Collectors.toSet())));
    }

    // TODO : verifier temporalite : intervalles etc. Difference d'objets : OffsetDateTime etc
    private void zipCgmsAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cgms, LocalDate localDate) {
        String cgmZipTmpDir = "/tmp/cgms_out/" + localDate.toString() + "/";
        // add cgm xml header to tmp folder
        raoIXmlResponseGenerator.generateCgmXmlHeaderFile(cgms.keySet(), cgmZipTmpDir, localDate);

        // Add all cgms from minio to tmp folder
        cgms.values().stream().filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED)).forEach(cgm -> {
            InputStream inputStream = minioAdapter.getFile(cgm.getFilePath().split("CORE/CC/")[1]);
            File cgmFile = new File(cgmZipTmpDir + cgm.getFilename());
            try {
                FileUtils.copyInputStreamToFile(inputStream, cgmFile);
            } catch (IOException e) {
                throw new CoreCCPostProcessingInternalException("error while copying cgm to tmp folder", e);
            }
        });

        // Zip tmp folder
        byte[] cgmsZipResult = ZipUtil.zipDirectory(cgmZipTmpDir);
        String targetCgmsFolderName = OutputFileNameUtil.generateCgmZipName(localDate);
        String targetCgmsFolderPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, targetCgmsFolderName);

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
                InputStream inputStream = minioAdapter.getFile(cne.getFilePath().split("CORE/CC/")[1]);
                File cneFile = new File(cneZipTmpDir + cne.getFilename());
                try {
                    FileUtils.copyInputStreamToFile(inputStream, cneFile);
                } catch (IOException e) {
                    throw new CoreCCPostProcessingInternalException("error while copying cne to tmp folder", e);
                }
            });

        byte[] cneZipResult = ZipUtil.zipDirectory(cneZipTmpDir);
        String targetCneFolderName = OutputFileNameUtil.generateCneZipName(localDate);
        String targetCneFolderPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, targetCneFolderName);

        try (InputStream cneZipIs = new ByteArrayInputStream(cneZipResult)) {
            minioAdapter.uploadOutput(targetCneFolderPath, cneZipIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while zipping CNEs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(cneZipTmpDir)); //NOSONAR
        }
    }

    private void zipLogsAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> logs, LocalDate localDate) {
        String logZipTmpDir = "/tmp/cnes_out/" + localDate.toString() + "/";

        // Add all cnes from minio to tmp folder
        logs.values().forEach(log -> {
            InputStream inputStream = minioAdapter.getFile(log.getFilePath());
            File logFile = new File(logZipTmpDir + log.getFilename());
            try {
                FileUtils.copyInputStreamToFile(inputStream, logFile);
            } catch (IOException e) {
                throw new CoreCCPostProcessingInternalException("error while copying cne to tmp folder", e);
            }
        });

        byte[] logZipResult = ZipUtil.zipDirectory(logZipTmpDir);
        String targetLogFolderName = OutputFileNameUtil.generateCneZipName(localDate);
        String targetLogFolderPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, targetLogFolderName);

        try (InputStream cneZipIs = new ByteArrayInputStream(logZipResult)) {
            minioAdapter.uploadOutput(targetLogFolderPath, cneZipIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while zipping CNEs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(logZipTmpDir)); //NOSONAR
        }
    }

    void uploadDailyOutputFlowBasedConstraintDocument(FlowBasedConstraintDocument dailyFbDocument, String targetMinioFolder, LocalDate localDate) {
        byte[] dailyFbConstraint = JaxbUtil.writeInBytes(FlowBasedConstraintDocument.class, dailyFbDocument);
        String fbConstraintFileName = OutputFileNameUtil.generateOptimizedCbFileName(localDate);
        String fbConstraintDestinationPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, fbConstraintFileName);

        try (InputStream dailyFbIs = new ByteArrayInputStream(dailyFbConstraint)) {
            minioAdapter.uploadOutput(fbConstraintDestinationPath, dailyFbIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while uploading F303 file of business day %s", localDate));
        }
    }
}
