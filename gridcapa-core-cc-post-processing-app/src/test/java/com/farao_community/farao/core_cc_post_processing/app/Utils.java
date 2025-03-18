/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.gridcapa.task_manager.api.ProcessEventDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessRunDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import com.farao_community.farao.minio_adapter.starter.MinioAdapterProperties;
import io.minio.MinioClient;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class Utils {

    public static final OffsetDateTime TIMESTAMP = OffsetDateTime.parse("2023-08-21T15:16:45Z");
    public static final ProcessFileDto CRAC_PROCESS_FILE = new ProcessFileDto("/CORE/CC/crac.xml", "CBCORA", ProcessFileStatus.VALIDATED, "crac.xml", "docId", TIMESTAMP);
    public static final ProcessFileDto CNE_FILE_DTO = new ProcessFileDto("/CORE/CC/cne.xml", "CNE", ProcessFileStatus.VALIDATED, "cne.xml", "docId", TIMESTAMP);
    public static final ProcessFileDto CNE_FILE_DTO_NOT_PRESENT = new ProcessFileDto("/CORE/CC/cne.xml", "CNE", ProcessFileStatus.VALIDATED, "cne.xml", "docId", TIMESTAMP);
    public static final ProcessFileDto CGM_FILE_DTO = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", "docId", TIMESTAMP);
    public static final ProcessFileDto CGM_FILE_DTO_NOT_PRESENT = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.NOT_PRESENT, "network.uct", "docId", TIMESTAMP);
    public static final ProcessFileDto METADATA_FILE_DTO = new ProcessFileDto("/CORE/CC/metadata.json", "METADATA", ProcessFileStatus.VALIDATED, "metadata.json", "docId", TIMESTAMP);
    public static final ProcessFileDto RAO_RESULT_FILE_DTO = new ProcessFileDto("/CORE/CC/raoResult.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult.json", "docId", TIMESTAMP);
    public static final ProcessFileDto RAO_RESULT_FILE_DTO_NOT_PRESENT = new ProcessFileDto("/CORE/CC/raoResult.json", "RAO_RESULT", ProcessFileStatus.NOT_PRESENT, "raoResult.json", "docId", TIMESTAMP);
    public static final List<ProcessFileDto> INPUTS = List.of(CRAC_PROCESS_FILE);
    public static final List<ProcessFileDto> OUTPUTS = List.of(CNE_FILE_DTO, CGM_FILE_DTO, METADATA_FILE_DTO, RAO_RESULT_FILE_DTO);
    public static final List<ProcessFileDto> OUTPUTS_CGM_NOT_PRESENT = List.of(CNE_FILE_DTO, CGM_FILE_DTO_NOT_PRESENT, METADATA_FILE_DTO, RAO_RESULT_FILE_DTO);
    public static final List<ProcessFileDto> OUTPUTS_NOT_PRESENT = List.of(CNE_FILE_DTO_NOT_PRESENT, CGM_FILE_DTO_NOT_PRESENT, METADATA_FILE_DTO, RAO_RESULT_FILE_DTO_NOT_PRESENT);
    public static final List<ProcessEventDto> PROCESS_EVENTS = List.of();
    public static final List<ProcessRunDto> PROCESS_RUN_DTOS_ONE = List.of(new ProcessRunDto(UUID.randomUUID(), OffsetDateTime.parse("2023-08-21T15:16:45Z"), INPUTS));
    public static final List<ProcessRunDto> PROCESS_RUN_DTOS_TWO = List.of(new ProcessRunDto(UUID.randomUUID(), OffsetDateTime.parse("2023-08-21T15:16:45Z"), INPUTS), new ProcessRunDto(UUID.randomUUID(), OffsetDateTime.parse("2023-08-22T15:16:45Z"), INPUTS));
    public static final TaskDto SUCCESS_TASK = new TaskDto(UUID.fromString("4fb56583-bcec-4ed9-9839-0984b7324989"), OffsetDateTime.parse("2023-08-21T15:16:45Z"), TaskStatus.SUCCESS, INPUTS, INPUTS, OUTPUTS, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto SUCCESS_TASK_CGM_NOT_PRESENT = new TaskDto(UUID.fromString("4fb56583-bcec-4ed9-9839-0984b7324989"), OffsetDateTime.parse("2023-08-21T15:16:45Z"), TaskStatus.SUCCESS, INPUTS, INPUTS, OUTPUTS_CGM_NOT_PRESENT, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto SUCCESS_TASK_NOT_PRESENT_STATUS = new TaskDto(UUID.fromString("4fb56583-bcec-4ed9-9839-0984b7324989"), OffsetDateTime.parse("2023-08-21T15:16:45Z"), TaskStatus.SUCCESS, INPUTS, INPUTS, OUTPUTS_NOT_PRESENT, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto ERROR_TASK = new TaskDto(UUID.fromString("6e3e0ef2-96e4-4649-82d4-374f103038d4"), OffsetDateTime.parse("2023-08-21T15:16:46Z"), TaskStatus.ERROR, INPUTS, INPUTS, OUTPUTS, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto ERROR_TASK_NO_METADATA = new TaskDto(UUID.fromString("6e3e0ef2-96e4-4649-82d4-374f103038d9"), OffsetDateTime.parse("2023-08-21T15:16:48Z"), TaskStatus.ERROR, INPUTS, INPUTS, OUTPUTS, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto ERROR_TASK_NOT_IN_RAO = new TaskDto(UUID.fromString("6e3e0ef2-96e4-4649-82d4-374f103038a1"), OffsetDateTime.parse("2023-08-21T15:16:49Z"), TaskStatus.ERROR, INPUTS, INPUTS, OUTPUTS, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    public static final TaskDto RUNNING_TASK = new TaskDto(UUID.fromString("b4efda15-92c5-431b-a17f-9c5f6d8a6437"), OffsetDateTime.parse("2023-08-21T15:16:47Z"), TaskStatus.RUNNING, INPUTS, INPUTS, OUTPUTS, PROCESS_EVENTS, PROCESS_RUN_DTOS_TWO, List.of());
    public static final CoreCCMetadata CORE_CC_METADATA_SUCCESS = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "6fe0a389-9315-417e-956d-b3fbaa479caz", "SUCCESS", "0", "This is an error.", 1);
    public static final CoreCCMetadata CORE_CC_METADATA_ERROR = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "6fe0a389-9315-417e-956d-b3fbaa479caz", "ERROR", "1", "This is an error.", 1);
    public static final CoreCCMetadata CORE_CC_METADATA_ERROR_NOT_IN_RAO = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "6fe0a389-9315-417e-956d-b3fbaa479caz", "ERROR", "1", "Missing raoRequest", 1);
    public static final CoreCCMetadata CORE_CC_METADATA_RUNNING = new CoreCCMetadata("raoRequest.json", "2023-08-04T11:26:00Z", "2023-08-04T11:26:00Z", "2023-08-04T11:27:00Z", "2023-08-04T11:29:00Z", "2023-08-04T11:25:00Z/2023-08-04T12:25:00Z", "6fe0a389-9315-417e-956d-b3fbaa479caz", "RUNNING", "0", "This is an error.", 1);
    private static final MinioAdapterProperties PROPERTIES = Mockito.mock(MinioAdapterProperties.class);
    private static final MinioClient MINIO_CLIENT = Mockito.mock(MinioClient.class);
    public static final MinioFileWriter MINIO_FILE_WRITER = new MinioFileWriter(PROPERTIES, MINIO_CLIENT);
    public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    public static TaskDto successTaskWithInputsAndOutputs(List<ProcessFileDto> inputs, List<ProcessFileDto> outputs) {
        return new TaskDto(UUID.fromString("4fb56583-bcec-4ed9-9839-0984b7324989"), OffsetDateTime.parse("2023-08-21T15:16:45Z"), TaskStatus.SUCCESS, inputs, inputs, outputs, PROCESS_EVENTS, PROCESS_RUN_DTOS_ONE, List.of());
    }

    public static void neutralizeCreationDate(File file, boolean isXml) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String regex = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z";
        String replacer = "yyyy-MM-ddTHH:mm:ss.SSSSSSZ";
        String pattern = isXml ? "<Timestamp>" + regex + "</Timestamp>" : regex;
        String replaceBy = isXml ? "<Timestamp>" + replacer + "</Timestamp>" : replacer;
        String line;
        StringBuilder oldText = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            oldText.append(line).append("\r\n");
        }
        reader.close();
        String newText = oldText.toString().replaceAll(pattern, replaceBy);
        FileWriter writer = new FileWriter(file.getAbsolutePath());
        writer.write(newText);
        writer.close();
    }

    public static void assertFilesContentEqual(String resource, String generatedFile, boolean removeCreationDate) throws IOException {
        if (removeCreationDate) {
            boolean isXml = resource.endsWith(".xml");
            neutralizeCreationDate(new File(generatedFile), isXml);
        }
        String expectedFileContents = new String(Utils.class.getResourceAsStream(resource).readAllBytes()).replace("\r", "");
        String actualFileContents = new String(Files.newInputStream(Paths.get(generatedFile)).readAllBytes()).replace("\r", "");
        assertEquals(expectedFileContents, actualFileContents);
    }

    public static boolean isFileContentEqualToString(final String result,
                                                     final String expectedResultPath) throws IOException {
        String expectedFileContents = new String(Utils.class.getResourceAsStream(expectedResultPath).readAllBytes()).replace("\r", "");
        return expectedFileContents.equals(result);
    }
}
