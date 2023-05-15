/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.springframework.stereotype.Service;

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

    /*public void generateRaoResponse(RaoIntegrationTask raoIntegrationTask, String targetMinioFolder, boolean isManualRun) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateRaoResponseHeader(raoIntegrationTask, responseMessage);
            generateRaoResponsePayLoad(raoIntegrationTask, responseMessage);
            exportRaoIntegrationResponse(raoIntegrationTask, responseMessage, targetMinioFolder, isManualRun);
        } catch (Exception e) {
            throw new RaoIntegrationException("Error occurred during Rao Integration Response file creation", e);
        }
    }

    public void generateCgmXmlHeaderFile(RaoIntegrationTask raoIntegrationTask, String cgmsTempDirPath) {
        try {
            ResponseMessageType responseMessage = new ResponseMessageType();
            generateCgmXmlHeaderFileHeader(raoIntegrationTask, responseMessage);
            generateCgmXmlHeaderFilePayLoad(raoIntegrationTask, responseMessage);
            exportCgmXmlHeaderFile(responseMessage, cgmsTempDirPath);
        } catch (Exception e) {
            throw new RaoIntegrationException("Error occurred during CGM_XML_HEADER creation", e);
        }
    }

    void generateRaoResponseHeader(RaoIntegrationTask raoIntegrationTask, ResponseMessageType responseMessage) throws DatatypeConfigurationException {
        HeaderType header = new HeaderType();
        header.setVerb("created");
        header.setNoun("OptimizedRemedialActions");
        header.setRevision(String.valueOf(raoIntegrationTask.getVersion()));
        header.setContext("PRODUCTION");
        header.setTimestamp(DatatypeFactory.newInstance().newXMLGregorianCalendar(Instant.now().toString()));
        header.setSource(SENDER_ID);
        header.setAsyncReplyFlag(false);
        header.setAckRequired(false);
        header.setReplyAddress(RECEIVER_ID);
        header.setMessageID(String.format("%s-%s-F305", SENDER_ID, IntervalUtil.getFormattedBusinessDay(raoIntegrationTask.getTimeInterval())));
        header.setCorrelationID(raoIntegrationTask.getCorrelationId());
        responseMessage.setHeader(header);
    }

    void generateCgmXmlHeaderFileHeader(RaoIntegrationTask raoIntegrationTask, ResponseMessageType responseMessage) throws DatatypeConfigurationException {
        HeaderType header = new HeaderType();
        header.setVerb("created");
        header.setNoun("OptimizedCommonGridModel");
        header.setContext("PRODUCTION");
        header.setRevision(String.valueOf(raoIntegrationTask.getVersion()));
        header.setSource(SENDER_ID);
        header.setReplyAddress(RECEIVER_ID);
        header.setTimestamp(DatatypeFactory.newInstance().newXMLGregorianCalendar(Instant.now().toString()));
        header.setCorrelationID(raoIntegrationTask.getCorrelationId());

        //need to save this MessageID and reuse in rao response
        String outputCgmXmlHeaderMessageId = String.format("%s-%s-F304v%s", SENDER_ID, IntervalUtil.getFormattedBusinessDay(raoIntegrationTask.getTimeInterval()), raoIntegrationTask.getVersion());
        header.setMessageID(outputCgmXmlHeaderMessageId);
        raoIntegrationTask.getDailyOutputs().setOutputCgmXmlHeaderMessageId(outputCgmXmlHeaderMessageId);

        responseMessage.setHeader(header);
    }

    private String formatInterval(Interval interval) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'").withZone(ZoneId.from(ZoneOffset.UTC));
        return String.format("%s/%s", formatter.format(interval.getStart()), formatter.format(interval.getEnd()));
    }

    void generateRaoResponsePayLoad(RaoIntegrationTask raoIntegrationTask, ResponseMessageType responseMessage) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(raoIntegrationTask.getTimeInterval());
        raoIntegrationTask.getHourlyRaoResults().stream().sorted(Comparator.comparing(HourlyRaoResult::getInstant))
                .forEach(hourlyRaoResult -> {
                    ResponseItem responseItem = new ResponseItem();
                    //set time interval
                    Instant instant = Instant.parse(hourlyRaoResult.getInstant());
                    Interval interval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
                    responseItem.setTimeInterval(formatInterval(interval));

                    if (hourlyRaoResult.getStatus().equals(TaskStatus.ERROR)) {
                        fillFailedHours(hourlyRaoResult, responseItem);
                    } else if (hourlyRaoResult.getStatus().equals(TaskStatus.RUNNING)) {
                        fillRunningHours(responseItem);
                    } else {
                        //set file
                        com.rte_france.farao.rao_integration.server.outputs.rao_response.Files files = new com.rte_france.farao.rao_integration.server.outputs.rao_response.Files();
                        com.rte_france.farao.rao_integration.server.outputs.rao_response.File file = new com.rte_france.farao.rao_integration.server.outputs.rao_response.File();

                        file.setCode(OPTIMIZED_CGM);
                        file.setUrl(DOCUMENT_IDENTIFICATION + raoIntegrationTask.getDailyOutputs().getOutputCgmXmlHeaderMessageId()); //MessageID of the CGM F304 zip (from header file)
                        files.getFile().add(file);

                        com.rte_france.farao.rao_integration.server.outputs.rao_response.File file1 = new com.rte_france.farao.rao_integration.server.outputs.rao_response.File();
                        file1.setCode(OPTIMIZED_CB);
                        file1.setUrl(DOCUMENT_IDENTIFICATION + raoIntegrationTask.getDailyOutputs().getOutputFlowBasedConstraintDocumentMessageId()); //MessageID of the f303
                        files.getFile().add(file1);

                        com.rte_france.farao.rao_integration.server.outputs.rao_response.File file2 = new com.rte_france.farao.rao_integration.server.outputs.rao_response.File();
                        file2.setCode(RAO_REPORT);
                        file2.setUrl(DOCUMENT_IDENTIFICATION + hourlyRaoResult.getCneResultDocumentId());
                        files.getFile().add(file2);

                        responseItem.setFiles(files);
                    }
                    responseItems.getResponseItem().add(responseItem);
                });
        PayloadType payload = new PayloadType();
        payload.setResponseItems(responseItems);
        responseMessage.setPayload(payload);
    }

    void generateCgmXmlHeaderFilePayLoad(RaoIntegrationTask raoIntegrationTask, ResponseMessageType responseMessage) {
        ResponseItems responseItems = new ResponseItems();
        responseItems.setTimeInterval(raoIntegrationTask.getTimeInterval());

        String[] timeInterval = raoIntegrationTask.getTimeInterval().split("/");
        Instant start = parseInstantWithoutSeconds(timeInterval[0]);
        Instant end = parseInstantWithoutSeconds(timeInterval[1]);
        for (Instant instant = start; instant.isBefore(end); instant = instant.plus(1, ChronoUnit.HOURS)) {
            final Instant instantFinal = instant;
            HourlyRaoResult hourlyRaoResult = raoIntegrationTask.getHourlyRaoResults().stream().filter(result -> result.getInstant().equals(instantFinal.toString())).findAny().orElse(null);
            ResponseItem responseItem = new ResponseItem();
            //set time interval
            Interval interval = Interval.of(instant, instant.plus(1, ChronoUnit.HOURS));
            responseItem.setTimeInterval(formatInterval(interval));

            if (hourlyRaoResult == null) {
                // If there's no result object then there was no request object
                fillMissingCgmInput(responseItem);
            } else if (hourlyRaoResult.getStatus().equals(TaskStatus.SUCCESS)) {
                //set file
                com.rte_france.farao.rao_integration.server.outputs.rao_response.Files files = new com.rte_france.farao.rao_integration.server.outputs.rao_response.Files();

                com.rte_france.farao.rao_integration.server.outputs.rao_response.File file = new com.rte_france.farao.rao_integration.server.outputs.rao_response.File();
                file.setCode(CGM);
                file.setUrl(FILENAME + OutputFileNameUtil.generateUctFileName(instant.toString(), raoIntegrationTask.getVersion()));
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

    private void fillFailedHours(HourlyRaoResult hourlyRaoResult, ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        error.setCode(hourlyRaoResult.getErrorCodeString());
        error.setLevel("FATAL");
        error.setReason(hourlyRaoResult.getErrorMessage());
        responseItem.setError(error);
    }

    private void fillRunningHours(ResponseItem responseItem) {
        ErrorType error = new ErrorType();
        error.setCode(HourlyRaoResult.ErrorCode.RUNNING.getCode());
        error.setLevel("INFORM");
        error.setReason("Running not finished yet");
        responseItem.setError(error);
    }

    private void exportRaoIntegrationResponse(RaoIntegrationTask raoIntegrationTask, ResponseMessageType responseMessage, String targetMinioFolder, boolean isManualRun) {
        byte[] responseMessageBytes = marshallMessageAndSetJaxbProperties(responseMessage);
        String raoIResponseFileName = OutputFileNameUtil.generateRaoIResponseFileName(raoIntegrationTask);
        String raoIntegrationResponseDestinationPath = OutputFileNameUtil.generateOutputsDestinationPath(targetMinioFolder, raoIResponseFileName);

        try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
            minioAdapter.uploadFile(raoIntegrationResponseDestinationPath, raoResponseIs);
            if (!isManualRun) {
                minioAdapter.copyObject(raoIntegrationResponseDestinationPath, raoIResponseFileName, minioAdapter.getDefaultBucket(), minioAdapter.getOutputsBucket());
            }
        } catch (IOException e) {
            throw new RaoIntegrationException(String.format("Exception occurred while uploading RAO response of task %s", raoIntegrationTask.getTaskId()));
        }
        raoIntegrationTask.getDailyOutputs().setRaoIntegrationResponsePath(raoIntegrationResponseDestinationPath);
    }

    private void exportCgmXmlHeaderFile(ResponseMessageType responseMessage, String cgmsArchiveTempPath) {
        try {
            byte[] responseMessageBytes = marshallMessageAndSetJaxbProperties(responseMessage);
            File targetFile = new File(cgmsArchiveTempPath, OutputsNamingRules.CGM_XML_HEADER_FILENAME); //NOSONAR

            try (InputStream raoResponseIs = new ByteArrayInputStream(responseMessageBytes)) {
                Files.copy(raoResponseIs, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

        } catch (IOException e) {
            throw new RaoIntegrationException("Exception occurred during CGM_XML_HEADER Response export.", e);
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
            throw new RaoIntegrationException("Exception occurred during RAO Response export.", e);
        }
    }*/
}
