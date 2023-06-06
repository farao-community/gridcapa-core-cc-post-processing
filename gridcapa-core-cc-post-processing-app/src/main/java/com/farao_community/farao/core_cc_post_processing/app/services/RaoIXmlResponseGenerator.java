/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.*;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.OutputFileNameUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.OutputsNamingRules;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.resource.HourlyRaoResult;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.namespace.QName;
import java.io.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed Ben Rejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
@Service
public class RaoIXmlResponseGenerator {

    private final MinioAdapter minioAdapter;
    public static final String OPTIMIZED_CGM = "OPTIMIZED_CGM";
    public static final String OPTIMIZED_CB = "OPTIMIZED_CB";
    public static final String RAO_REPORT = "RAO_REPORT";
    public static final String CGM = "CGM";
    public static final String FILENAME = "fileName://";
    public static final String DOCUMENT_IDENTIFICATION = "documentIdentification://";
    public static final String SENDER_ID = "22XCORESO------S";
    public static final String RECEIVER_ID = "17XTSO-CS------W";

    public RaoIXmlResponseGenerator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public void generateRaoResponse(Set<TaskDto> taskDtos, String targetMinioFolder, LocalDate localDate, String correlationId, Map<TaskDto, ProcessFileDto> metadatas) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateRaoResponseHeader(responseMessage, localDate, correlationId);
            generateRaoResponsePayLoad(taskDtos, responseMessage, localDate);
            exportRaoIntegrationResponse(responseMessage, targetMinioFolder, localDate);
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Error occurred during Rao Integration Response file creation", e);
        }
    }

    public void generateCgmXmlHeaderFile(Set<TaskDto> taskDtos, String cgmsTempDirPath, LocalDate localDate) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateCgmXmlHeaderFileHeader(responseMessage, localDate);
            generateCgmXmlHeaderFilePayLoad(taskDtos, responseMessage, localDate);
            exportCgmXmlHeaderFile(responseMessage, cgmsTempDirPath);
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Error occurred during CGM_XML_HEADER creation", e);
        }
    }

    // Attention, erreur de checkstyle
    void generateRaoResponseHeader(ResponseMessageType responseMessage, LocalDate localDate, String correlationId) throws DatatypeConfigurationException {
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

    void generateCgmXmlHeaderFileHeader(ResponseMessageType responseMessage, LocalDate localDate) throws DatatypeConfigurationException {
        HeaderType header = new HeaderType();
        header.setVerb("created");
        header.setNoun("OptimizedCommonGridModel");
        header.setContext("PRODUCTION");
        header.setRevision(String.valueOf(1));
        header.setSource(SENDER_ID);
        header.setReplyAddress(RECEIVER_ID);
        header.setTimestamp(DatatypeFactory.newInstance().newXMLGregorianCalendar(Instant.now().toString()));
        //TODO:header.setCorrelationID(taskDto.getCorrelationId());

        //need to save this MessageID and reuse in rao response
        String outputCgmXmlHeaderMessageId = String.format("%s-%s-F304v%s", SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
        header.setMessageID(outputCgmXmlHeaderMessageId);

        responseMessage.setHeader(header);
    }

    private String formatInterval(Interval interval) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'").withZone(ZoneId.from(ZoneOffset.UTC));
        return String.format("%s/%s", formatter.format(interval.getStart()), formatter.format(interval.getEnd()));
    }

    void generateRaoResponsePayLoad(Set<TaskDto> taskDtos, ResponseMessageType responseMessage, LocalDate localDate) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(IntervalUtil.getFormattedBusinessDay(localDate));
        taskDtos.stream().sorted(Comparator.comparing(TaskDto::getTimestamp))
                .forEach(taskDto -> {
                    ResponseItem responseItem = new ResponseItem();
                    //set time interval
                    Instant instant = taskDto.getTimestamp().toInstant();
                    Interval interval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
                    responseItem.setTimeInterval(formatInterval(interval));

                    if (taskDto.getStatus().equals(TaskStatus.ERROR)) {
                        fillFailedHours(taskDto, responseItem);
                    } else if (taskDto.getStatus().equals(TaskStatus.RUNNING)) {
                        fillRunningHours(responseItem);
                    } else {
                        //set file
                        com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files files = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files();
                        com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();

                        file.setCode(OPTIMIZED_CGM);
                        String outputCgmXmlHeaderMessageId = String.format("%s-%s-F304v%s", SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
                        file.setUrl(DOCUMENT_IDENTIFICATION + outputCgmXmlHeaderMessageId); //MessageID of the CGM F304 zip (from header file)
                        files.getFile().add(file);

                        com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file1 = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();
                        file1.setCode(OPTIMIZED_CB);
                        String outputFlowBasedConstraintDocumentMessageId = String.format("%s-%s-F304v%s", SENDER_ID, IntervalUtil.getFormattedBusinessDay(localDate), 1);
                        file1.setUrl(DOCUMENT_IDENTIFICATION + outputFlowBasedConstraintDocumentMessageId); //MessageID of the f303
                        files.getFile().add(file1);

                        com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file2 = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();
                        file2.setCode(RAO_REPORT);
                        String networkFileName = taskDto.getOutputs().stream().filter(processFileDto -> processFileDto.getFileType().equals("CGM_OUT"))
                            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException(String.format("Cannot find network file for task %s", taskDto.getTimestamp())))
                            .getFilename();
                        file2.setUrl(DOCUMENT_IDENTIFICATION + networkFileName);
                        files.getFile().add(file2);

                        responseItem.setFiles(files);
                    }
                    responseItems.getResponseItem().add(responseItem);
                });
        PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    void generateCgmXmlHeaderFilePayLoad(Set<TaskDto> taskDtos, ResponseMessageType responseMessage, LocalDate localDate) {
        ResponseItems responseItems = new ResponseItems();
        Interval interval = IntervalUtil.getInterval(localDate);
        responseItems.setTimeInterval(interval.toString());

        Instant start = interval.getStart();
        Instant end = interval.getEnd();
        for (Instant instant = start; instant.isBefore(end); instant = instant.plus(1, ChronoUnit.HOURS)) {
            Interval hourInterval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));

            TaskDto taskDto = taskDtos.stream().filter(task -> hourInterval.contains(task.getTimestamp().toInstant())).findAny().orElse(null);
            ResponseItem responseItem = new ResponseItem();
            //set time interval
            responseItem.setTimeInterval(formatInterval(interval));

            if (taskDto == null) {
                // If there's no result object then there was no request object
                fillMissingCgmInput(responseItem);
            } else if (taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
                //set file
                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files files = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.Files();

                com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File file = new com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.File();
                file.setCode(CGM);
                file.setUrl(FILENAME + OutputFileNameUtil.generateUctFileName(instant.toString(), 1));
                files.getFile().add(file);
                responseItem.setFiles(files);
            }
            responseItems.getResponseItem().add(responseItem);
        }
        PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    private Instant parseInstantWithoutSeconds(String instant) {
        return Instant.parse(instant.replace(":00Z", ":00:00Z"));
    }

    private void fillMissingCgmInput(ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        error.setCode("NOT_AVAILABLE");
        error.setReason("UCT file is not available for this time interval");
        responseItem.setError(error);
    }

    private void fillFailedHours(TaskDto taskDto, ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        //TODO:error.setCode(taskDto.getErrorCodeString());
        error.setLevel("FATAL");
        //TODO:error.setReason(taskDto.getErrorMessage());
        responseItem.setError(error);
    }

    private void fillRunningHours(ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        error.setCode(HourlyRaoResult.ErrorCode.RUNNING.getCode());
        error.setLevel("INFORM");
        error.setReason("Running not finished yet");
        responseItem.setError(error);
    }

    private void exportRaoIntegrationResponse(ResponseMessageType responseMessage, String targetMinioFolder, LocalDate localDate) {
        byte[] responseMessageBytes = marshallMessageAndSetJaxbProperties(responseMessage);
        String raoIResponseFileName = OutputFileNameUtil.generateRaoIResponseFileName(localDate);
        String raoIntegrationResponseDestinationPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, raoIResponseFileName);

        try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
            minioAdapter.uploadOutput(raoIntegrationResponseDestinationPath, raoResponseIs);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while uploading RAO response for business date %s", localDate));
        }
    }

    private void exportCgmXmlHeaderFile(ResponseMessageType responseMessage, String cgmsArchiveTempPath) {
        try {
            byte[] responseMessageBytes = marshallMessageAndSetJaxbProperties(responseMessage);
            File targetFile = new File(cgmsArchiveTempPath, OutputsNamingRules.CGM_XML_HEADER_FILENAME); //NOSONAR

            try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
                Files.copy(raoResponseIs, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during CGM_XML_HEADER Response export.", e);
        }
    }

    private byte[] marshallMessageAndSetJaxbProperties(ResponseMessageType responseMessage) {
        try {
            StringWriter stringWriter = new StringWriter();
            JAXBContext jaxbContext = JAXBContext.newInstance(ResponseMessageType.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            String eventMessage = "EventMessage";
            QName qName = new QName(XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, eventMessage);
            JAXBElement<ResponseMessageType> root = new JAXBElement<>(qName, ResponseMessageType.class, responseMessage);
            jaxbMarshaller.marshal(root, stringWriter);
            return stringWriter.toString()
                    .replace("xsi:EventMessage", "EventMessage")
                    .replace("<EventMessage", "<EventMessage xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"")
                    .replace("<ResponseItems", "<ResponseItems xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://unicorn.com/Response/response-payload\"")
                    .getBytes();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during RAO Response export.", e);
        }
    }
}
