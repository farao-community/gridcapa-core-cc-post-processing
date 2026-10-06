/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ErrorType;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.HeaderType;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.PayloadType;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseItem;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseItems;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.threeten.extra.Interval;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed Ben Rejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public final class RaoResponseXmlGenerator {
    private static final String F299_PATH = "%s-%s-F299v%s";
    private static final String F303_PATH = "%s-%s-F303v%s";
    private static final String F304_PATH = "%s-%s-F304v%s";
    private static final String OPTIMIZED_CGM = "OPTIMIZED_CGM";
    private static final String OPTIMIZED_CB = "OPTIMIZED_CB";
    private static final String RAO_REPORT = "RAO_REPORT";
    private static final String DOCUMENT_IDENTIFICATION = "documentIdentification://";
    private static final String SENDER_ID = "22XCORESO------S";
    private static final String RECEIVER_ID = "17XTSO-CS------W";
    private static final String INTERNAL_EXCEPTION = "500-InternalException";
    private static final String NO_OUTPUT_AVAILABLE = "No output available";
    private static final String MISSING_RAO_REQUEST_ERROR_MESSAGE = "Missing raoRequest";
    private static final String CONTINENTAL_PREFIX = "[CONTINENTAL] %s";
    private static final String SEM_PREFIX = "[SEM] %s";

    private RaoResponseXmlGenerator() {
    }

    public static ResponseMessageType generateRaoResponse(final Set<TaskDto> taskDtos,
                                                          final Map<TaskDto, ProcessFileDto> cgmPerTask,
                                                          final LocalDate localDate,
                                                          final String correlationId,
                                                          final Map<UUID, CoreCCMetadata> metadataMap,
                                                          final String timeInterval) {
        try {
            final ResponseMessageType responseMessage = new ResponseMessageType();
            generateRaoResponseHeader(responseMessage, localDate, correlationId);
            generateRaoResponsePayload(taskDtos, cgmPerTask, responseMessage, localDate, metadataMap, timeInterval);
            return responseMessage;
        } catch (final Exception e) {
            throw new CoreCCPostProcessingInternalException("Error occurred during RAO response file creation", e);
        }
    }

    private static void generateRaoResponseHeader(final ResponseMessageType responseMessage,
                                                  final LocalDate localDate,
                                                  final String correlationId) throws DatatypeConfigurationException {
        final HeaderType header = new HeaderType();
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

    private static void generateRaoResponsePayload(final Set<TaskDto> taskDtos,
                                                   final Map<TaskDto, ProcessFileDto> cgmPerTask,
                                                   final ResponseMessageType responseMessage,
                                                   final LocalDate localDate,
                                                   final Map<UUID, CoreCCMetadata> metadataMap,
                                                   final String timeInterval) {
        final ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(timeInterval);
        taskDtos.stream()
            .sorted(Comparator.comparing(TaskDto::getTimestamp))
            .forEach(taskDto -> generateRaoResponsePayloadForTask(taskDto, cgmPerTask, localDate, metadataMap, responseItems));
        final PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    private static void generateRaoResponsePayloadForTask(final TaskDto taskDto,
                                                          final Map<TaskDto, ProcessFileDto> cgmPerTask,
                                                          final LocalDate localDate,
                                                          final Map<UUID, CoreCCMetadata> metadataMap,
                                                          final ResponseItems responseItems) {
        final ResponseItem responseItem = new ResponseItem();
        //set time interval to [taskDto - 30 minutes, taskDto + 30 minutes] (taskDto has a timestamp of x:30 but we want x:00 - y:00)
        final Instant instant = taskDto.getTimestamp().toInstant().minus(30, ChronoUnit.MINUTES);
        final Interval interval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
        responseItem.setTimeInterval(IntervalUtil.formatIntervalInUtc(interval));
        boolean includeResponseItem = true;

        if (taskDto.getStatus().equals(TaskStatus.ERROR)) {
            if (!metadataMap.containsKey(taskDto.getId())) {
                fillFailedHours(responseItem, INTERNAL_EXCEPTION, NO_OUTPUT_AVAILABLE, true);
            } else if (isRaoRequestMissing(taskDto, metadataMap)) {
                // Do not generate a responseItem : raoRequest was not defined for this timestamp
                includeResponseItem = false;
            } else if (!cgmPerTask.containsKey(taskDto)) {
                fillFailedHours(responseItem, "CGM", "", false);
            } else {
                final CoreCCMetadata metadata = metadataMap.get(taskDto.getId());
                final List<String> errorCodes = new ArrayList<>();
                final List<String> errorMessages = new ArrayList<>();
                if (metadata.getContinentalComputationStatus() != null) {
                    errorCodes.add(String.format(CONTINENTAL_PREFIX, metadata.getContinentalComputationErrorCode()));
                    errorMessages.add(String.format(CONTINENTAL_PREFIX, metadata.getContinentalComputationErrorMessage()));
                }
                if (metadata.getSemComputationStatus() != null) {
                    errorCodes.add(String.format(SEM_PREFIX, metadata.getSemComputationErrorCode()));
                    errorMessages.add(String.format(SEM_PREFIX, metadata.getSemComputationErrorMessage()));
                }
                final String errorCode = String.join(" ; ", errorCodes);
                final String errorMessage = String.join(" ; ", errorMessages);
                fillFailedHours(responseItem, errorCode, errorMessage, true);
            }
        } else {
            generateRaoResponsePayloadForSuccessfulTask(localDate, responseItem);
        }
        if (includeResponseItem) {
            responseItems.getResponseItem().add(responseItem);
        }
    }

    private static boolean isRaoRequestMissing(final TaskDto taskDto, final Map<UUID, CoreCCMetadata> metadataMap) {
        final CoreCCMetadata taskMetadata = metadataMap.get(taskDto.getId());
        return MISSING_RAO_REQUEST_ERROR_MESSAGE.equals(taskMetadata.getContinentalComputationErrorMessage())
            || MISSING_RAO_REQUEST_ERROR_MESSAGE.equals(taskMetadata.getSemComputationErrorMessage());
    }

    private static void generateRaoResponsePayloadForSuccessfulTask(final LocalDate localDate, final ResponseItem responseItem) {
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

    private static void fillFailedHours(ResponseItem responseItem, String errorCode, String errorMessage, boolean withFatalLevel) {
        ErrorType error = new ErrorType();
        error.setCode(errorCode);
        if (withFatalLevel) {
            error.setLevel("FATAL");
        }
        error.setReason(errorMessage);
        responseItem.setError(error);
    }
}
