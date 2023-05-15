package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.services.DailyF303Generator;
import com.farao_community.farao.core_cc_post_processing.app.services.FileExporterHelper;
import com.farao_community.farao.core_cc_post_processing.app.services.RaoIXmlResponseGenerator;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.springframework.stereotype.Service;

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
        zipCgmsAndSendToOutputs(outputsTargetMinioFolder, cgmPerTask);
        zipCnesAndSendToOutputs(outputsTargetMinioFolder, cnePerTask);
        /*renameRaoHourlyResultsAndSendToDailyOutputs(raoIntegrationTask, outputsTargetMinioFolder, isManualRun);
        FlowBasedConstraintDocument dailyFlowBasedConstraintDocument = dailyF303Generator.generate(raoIntegrationTask);
        uploadDailyOutputFlowBasedConstraintDocument(raoIntegrationTask, dailyFlowBasedConstraintDocument, outputsTargetMinioFolder, isManualRun);
        raoIXmlResponseGenerator.generateRaoResponse(raoIntegrationTask, outputsTargetMinioFolder, isManualRun); //f305 rao response
        raoIntegrationTask.setOutputsSendingInstant(Instant.now());
        raoIntegrationTask.setTaskStatus(TaskStatus.SUCCESS); // status success should be set before exportMetadataFile because it's displayed within it
        fileExporterHelper.exportMetadataFile(raoIntegrationTask, outputsTargetMinioFolder, isManualRun);*/
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
                    case "CGM":
                        cgms.put(taskDto, processFileDto);
                        break;
                }
            })
        );
    }

    private void zipCgmsAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cgms) {

    }

    private void zipCnesAndSendToOutputs(String targetMinioFolder, Map<TaskDto, ProcessFileDto> cnes) {

    }
}
