package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.core_cc_post_processing.app.services.FileExporterHelper;
import com.farao_community.farao.core_cc_post_processing.app.services.RaoIXmlResponseGenerator;
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.OutputFileNameUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.ZipUtil;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Service
public class PostProcessingService {

    private final MinioAdapter minioAdapter;
    private final RaoIXmlResponseGenerator raoIXmlResponseGenerator;
    private final DailyF303Generator dailyF303Generator;
    private final FileExporterHelper fileExporterHelper;

    public PostProcessingService(MinioAdapter minioAdapter, RaoIXmlResponseGenerator raoIXmlResponseGenerator, DailyF303Generator dailyF303Generator, FileExporterHelper fileExporterHelper) {
        this.minioAdapter = minioAdapter;
        this.raoIXmlResponseGenerator = raoIXmlResponseGenerator;
        this.dailyF303Generator = dailyF303Generator;
        this.fileExporterHelper = fileExporterHelper;
    }

    public void processTasks(LocalDate localDate, Set<TaskDto> tasksToPostProcess) {
        String outputsTargetMinioFolder = generateTargetMinioFolder(localDate);
        Map<TaskDto, ProcessFileDto> cnePerTask = new HashMap<>();
        Map<TaskDto, ProcessFileDto> cgmPerTask = new HashMap<>();
        fillMapsOfOutputs(tasksToPostProcess, cnePerTask, cgmPerTask);
        zipCgmsAndSendToOutputs(outputsTargetMinioFolder, cgmPerTask, localDate);
        zipCnesAndSendToOutputs(outputsTargetMinioFolder, cnePerTask, localDate);
        //TODO: zip logs
        FlowBasedConstraintDocument dailyFlowBasedConstraintDocument = dailyF303Generator.generate(tasksToPostProcess);
        uploadDailyOutputFlowBasedConstraintDocument(dailyFlowBasedConstraintDocument, outputsTargetMinioFolder, localDate);
        //TODO: get correlation id
        raoIXmlResponseGenerator.generateRaoResponse(tasksToPostProcess, outputsTargetMinioFolder, localDate, "correlationId"); //f305 rao response
        //TODO: generate metadata file
    }

    private String generateTargetMinioFolder(LocalDate localDate) {
        return "RAO_OUTPUT_DIR/" + localDate;
    }

    private void fillMapsOfOutputs(Set<TaskDto> tasksToProcess,
                                   Map<TaskDto, ProcessFileDto> cnes,
                                   Map<TaskDto, ProcessFileDto> cgms) {
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
                }
            })
        );
    }

    private void zipCgmsAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cgms, LocalDate localDate) {
        String cgmZipTmpDir = "/tmp/cgms_out/" + localDate.toString();
        // add cgm xml header to tmp folder
        raoIXmlResponseGenerator.generateCgmXmlHeaderFile(cgms.keySet(), cgmZipTmpDir, localDate);

        // Add all cgms from minio to tmp folder
        cgms.values().forEach(cgm -> {
            InputStream inputStream = minioAdapter.getFile(cgm.getFilePath());
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
        String cneZipTmpDir = "/tmp/cnes_out/" + localDate.toString();

        // Add all cnes from minio to tmp folder
        cnes.values().forEach(cne -> {
            InputStream inputStream = minioAdapter.getFile(cne.getFilePath());
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
