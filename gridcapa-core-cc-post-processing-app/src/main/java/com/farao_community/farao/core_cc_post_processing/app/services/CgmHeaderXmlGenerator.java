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
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.NamingRules;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import org.threeten.extra.Interval;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Set;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed Ben Rejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public final class CgmHeaderXmlGenerator {
    private static final String F304_PATH = "%s-%s-F304v%s";
    private static final String CGM = "CGM";
    private static final String FILENAME = "fileName://";
    private static final String SENDER_ID = "22XCORESO------S";
    private static final String RECEIVER_ID = "17XTSO-CS------W";

    private CgmHeaderXmlGenerator() {
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

    static void generateCgmXmlHeaderFilePayLoad(Set<TaskDto> taskDtos, ResponseMessageType responseMessage, String timeInterval) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(timeInterval);

        String[] splitTimeInterval = timeInterval.split("/");
        Instant start = parseInstantWithoutSeconds(splitTimeInterval[0]);
        Instant end = parseInstantWithoutSeconds(splitTimeInterval[1]);

        for (Instant instant = start; instant.isBefore(end); instant = instant.plus(1, ChronoUnit.HOURS)) {
            Interval hourInterval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
            TaskDto taskDto = taskDtos.stream()
                    .filter(task -> hourInterval.contains(task.getTimestamp().toInstant()))
                    .findAny()
                    .orElse(null);
            ResponseItem responseItem = new ResponseItem();
            //set time interval
            responseItem.setTimeInterval(IntervalUtil.formatIntervalInUtc(hourInterval));

            if (taskDto == null) {
                fillMissingCgmOutput(responseItem);
            } else if (taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
                //set file
                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files files = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files();
                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();

                file.setCode(CGM);
                file.setUrl(FILENAME + NamingRules.generateCgmFilename(instant.toString(), 1));
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

    private static void fillMissingCgmOutput(final ResponseItem responseItem) {
        final ErrorType error = new ErrorType();
        error.setCode("CGM");
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
