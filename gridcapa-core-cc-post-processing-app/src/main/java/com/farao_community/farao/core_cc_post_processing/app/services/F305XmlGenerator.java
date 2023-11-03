/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.*;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.NamingRules;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInvalidDataException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed Ben Rejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public final class F305XmlGenerator {

    public static final String F299_PATH = "%s-%s-F299v%s";
    public static final String F303_PATH = "%s-%s-F303v%s";
    public static final String F304_PATH = "%s-%s-F304v%s";
    public static final String OPTIMIZED_CGM = "OPTIMIZED_CGM";
    public static final String OPTIMIZED_CB = "OPTIMIZED_CB";
    public static final String RAO_REPORT = "RAO_REPORT";
    public static final String CGM = "CGM";
    public static final String FILENAME = "fileName://";
    public static final String DOCUMENT_IDENTIFICATION = "documentIdentification://";
    public static final String SENDER_ID = "22XCORESO------S";
    public static final String RECEIVER_ID = "17XTSO-CS------W";

    private F305XmlGenerator() {
    }

    public static ResponseMessageType generateRaoResponse(Set<TaskDto> taskDtos, LocalDate localDate, String correlationId, Map<UUID, CoreCCMetadata> metadataMap, String timeInterval) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateRaoResponseHeader(responseMessage, localDate, correlationId);
            generateRaoResponsePayLoad(taskDtos, responseMessage, localDate, metadataMap, timeInterval);
            return responseMessage;
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Error occurred during F305 file creation", e);
        }
    }

    public static void generateCgmXmlHeaderFile(Set<TaskDto> taskDtos, String cgmsTempDirPath, LocalDate localDate, String correlationId, String timeInterval) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateCgmXmlHeaderFileHeader(responseMessage, localDate, correlationId);
            generateCgmXmlHeaderFilePayLoad(taskDtos, responseMessage, timeInterval);
            exportCgmXmlHeaderFile(responseMessage, cgmsTempDirPath);
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Error occurred during CGM_XML_HEADER creation", e);
        }
    }

    private static void generateRaoResponseHeader(ResponseMessageType responseMessage, LocalDate localDate, String correlationId) throws DatatypeConfigurationException {
        HeaderType header = new HeaderType();
        header.setVerb("created");
        header.setNoun("OptimizedRemedialActions");
        header.setRevision(String.valueOf(1));
        header.setContext("PRODUCTION");
        header.setTimestamp(DatatypeFactory.newInstance().newXMLGregorianCalendar(Instant.now().toString()));
        header.setSource(SENDER_ID);
        header.setAsyncReplyFlag(false);
        header.setAckRequired(false);
        header.setReplyAddress(RECEIVER_ID);
        header.setMessageID(String.format("%s-%s-F305", SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate)));
        header.setCorrelationID(correlationId);
        responseMessage.setHeader(header);
    }

    private static void generateCgmXmlHeaderFileHeader(ResponseMessageType responseMessage, LocalDate localDate, String correlationId) throws DatatypeConfigurationException {
        HeaderType header = new HeaderType();
        header.setVerb("created");
        header.setNoun("OptimizedCommonGridModel");
        header.setContext("PRODUCTION");
        header.setRevision(String.valueOf(1));
        header.setSource(SENDER_ID);
        header.setReplyAddress(RECEIVER_ID);
        header.setTimestamp(DatatypeFactory.newInstance().newXMLGregorianCalendar(Instant.now().toString()));
        header.setCorrelationID(correlationId);

        //need to save this MessageID and reuse in rao response
        String outputCgmXmlHeaderMessageId = String.format(F304_PATH, SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
        header.setMessageID(outputCgmXmlHeaderMessageId);

        responseMessage.setHeader(header);
    }

    private static void generateRaoResponsePayLoad(Set<TaskDto> taskDtos, ResponseMessageType responseMessage, LocalDate localDate, Map<UUID, CoreCCMetadata> metadataMap, String timeInterval) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(timeInterval);
        taskDtos.stream().sorted(Comparator.comparing(TaskDto::getTimestamp))
            .forEach(taskDto -> {
                ResponseItem responseItem = new ResponseItem();
                //set time interval to [taskDto - 30 minutes, taskDto + 30 minutes] (taskDto has a timestamp of x:30 but we want x:00 - y:00)
                Instant instant = taskDto.getTimestamp().toInstant().minus(30, ChronoUnit.MINUTES);
                Interval interval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
                responseItem.setTimeInterval(IntervalUtil.formatIntervalInUtc(interval));
                boolean includeResponseItem = true;

                if (taskDto.getStatus().equals(TaskStatus.ERROR)) {
                    if (!metadataMap.containsKey(taskDto.getId())) {
                        throw new CoreCCInvalidDataException(String.format("Wrong task id : %s not in metadataMap", taskDto.getId()));
                    }
                    if (metadataMap.get(taskDto.getId()).getErrorMessage().equals("Missing raoRequest")) {
                        // Do not generate a responseItem : raoRequest was not defined for this timestamp
                        includeResponseItem = false;
                    } else {
                        fillFailedHours(responseItem, metadataMap.get(taskDto.getId()).getErrorCode(), metadataMap.get(taskDto.getId()).getErrorMessage());
                    }
                } else {
                    //set file
                    com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files files = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files();
                    com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();

                    file.setCode(OPTIMIZED_CGM);
                    String outputCgmXmlHeaderMessageId = String.format(F304_PATH, SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
                    file.setUrl(DOCUMENT_IDENTIFICATION + outputCgmXmlHeaderMessageId); //MessageID of the CGM F304 zip (from header file)
                    files.getFile().add(file);

                    com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file1 = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();
                    file1.setCode(OPTIMIZED_CB);
                    String outputFlowBasedConstraintDocumentMessageId = String.format(F303_PATH, SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
                    file1.setUrl(DOCUMENT_IDENTIFICATION + outputFlowBasedConstraintDocumentMessageId); //MessageID of the f303
                    files.getFile().add(file1);

                    com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file2 = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();
                    file2.setCode(RAO_REPORT);
                    String outputLogsDocumentMessageId = String.format(F299_PATH, SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
                    file2.setUrl(DOCUMENT_IDENTIFICATION + outputLogsDocumentMessageId); //MessageID of the f299
                    files.getFile().add(file2);

                    responseItem.setFiles(files);
                }
                if (includeResponseItem) {
                    responseItems.getResponseItem().add(responseItem);
                }
            });
        PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    private static void generateCgmXmlHeaderFilePayLoad(Set<TaskDto> taskDtos, ResponseMessageType responseMessage, String timeInterval) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(timeInterval);

        String[] splitTimeInterval = timeInterval.split("/");
        Instant start = parseInstantWithoutSeconds(splitTimeInterval[0]);
        Instant end = parseInstantWithoutSeconds(splitTimeInterval[1]);

        for (Instant instant = start; instant.isBefore(end); instant = instant.plus(1, ChronoUnit.HOURS)) {
            Interval hourInterval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
            TaskDto taskDto = taskDtos.stream().filter(task -> hourInterval.contains(task.getTimestamp().toInstant())).findAny().orElse(null);
            ResponseItem responseItem = new ResponseItem();
            //set time interval
            responseItem.setTimeInterval(IntervalUtil.formatIntervalInUtc(hourInterval));

            if (taskDto == null) {
                // If there's no result object then there was no request object
                fillMissingCgmInput(responseItem);
            } else if (taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
                //set file
                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files files = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files();
                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();

                file.setCode(CGM);
                file.setUrl(FILENAME + NamingRules.generateUctFileName(instant.toString(), 1));
                files.getFile().add(file);
                responseItem.setFiles(files);
            }
            responseItems.getResponseItem().add(responseItem);
        }
        PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    private static Instant parseInstantWithoutSeconds(String instant) {
        return Instant.parse(instant.replace(":00Z", ":00:00Z"));
    }

    private static void fillMissingCgmInput(ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        error.setCode("NOT_AVAILABLE");
        error.setReason("UCT file is not available for this time interval");
        responseItem.setError(error);
    }

    private static void fillFailedHours(ResponseItem responseItem, String errorCode, String errorMessage) {
        ErrorType error = new ErrorType();
        error.setCode(errorCode);
        error.setLevel("FATAL");
        error.setReason(errorMessage);
        responseItem.setError(error);
    }

    private static void exportCgmXmlHeaderFile(ResponseMessageType responseMessage, String cgmsArchiveTempPath) {
        try {
            byte[] responseMessageBytes = JaxbUtil.marshallMessageAndSetJaxbProperties(responseMessage);
            File targetFile = new File(cgmsArchiveTempPath, NamingRules.CGM_XML_HEADER_FILENAME); //NOSONAR

            if (!Files.exists(targetFile.getParentFile().toPath())) {
                targetFile.getParentFile().mkdirs();
            }

            try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
                Files.copy(raoResponseIs, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during CGM_XML_HEADER Response export.", e);
        }
    }
}
