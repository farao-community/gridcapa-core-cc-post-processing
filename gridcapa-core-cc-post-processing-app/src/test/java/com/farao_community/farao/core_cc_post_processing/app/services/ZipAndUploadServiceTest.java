/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.craccreation.creator.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.CGM_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.CGM_FILE_DTO_NOT_PRESENT;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.CNE_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.RAO_RESULT_FILE_DTO;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.RAO_RESULT_FILE_DTO_NOT_PRESENT;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK_NOT_PRESENT_STATUS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ZipAndUploadServiceTest {

    @Mock
    private MinioAdapter minioAdapterMock;

    @InjectMocks
    private ZipAndUploadService zipAndUploadService;
    private static final LocalDate LOCAL_DATE = LocalDate.of(2023, 8, 4);
    private static final String TARGET_FOLDER = "targetFolder";

    @Test
    void testZipAndUploadLogs() throws IOException {
        List<byte[]> logList = List.of(fileToByteArray("/services/export/logs1.txt"));
        zipAndUploadService.zipAndUploadLogs(logList, "logFileName");
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
    }

    @Test
    void testZipAndUploadLogsWhenExceptionThrown() throws IOException {
        List<byte[]> logList = List.of(fileToByteArray("/services/export/logs1.txt"));
        doThrow(CoreCCPostProcessingInternalException.class)
                .when(minioAdapterMock)
                .uploadOutput(anyString(), any(InputStream.class));
        Assertions.assertThrows(CoreCCPostProcessingInternalException.class,
                () -> zipAndUploadService.zipAndUploadLogs(logList, "logFileName"));

    }

    private byte[] fileToByteArray(final String filename) throws IOException {
        return getClass().getResourceAsStream(filename).readAllBytes();
    }

    // ------------ CGMES ------------

    @Test
    void testZipValidatedCgmsAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> cgms = new HashMap<>();
        cgms.put(SUCCESS_TASK, CGM_FILE_DTO);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/network.uct"))
                .thenReturn(getClass().getResourceAsStream("/services/network.uct"));
        zipAndUploadService.zipCgmsAndSendToOutputs(TARGET_FOLDER,
                cgms,
                LOCAL_DATE,
                "00000000-0000-0000-0000-000000000000",
                "2019-01-07T23:00Z/2019-01-08T23:00Z",
                1);
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));

        assertFalse(new File("/tmp/cgms_out/2023-08-04").exists());
    }

    @Test
    void testZipCgmsNotPresentAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> cgms = new HashMap<>();
        cgms.put(SUCCESS_TASK_NOT_PRESENT_STATUS, CGM_FILE_DTO_NOT_PRESENT);

        zipAndUploadService.zipCgmsAndSendToOutputs(TARGET_FOLDER,
                cgms,
                LOCAL_DATE,
                "00000000-0000-0000-0000-000000000000",
                "2019-01-07T23:00Z/2019-01-08T23:00Z",
                1);
        //In case of NOT ProcessFileStatus == VALIDATED, should not retrieve cgm from minio
        verify(minioAdapterMock, never()).getFileFromFullPath(anyString());
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
    }

    // ------------ CNES ------------
    @Test
    void testZipCnesAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> cnes = new HashMap<>();
        cnes.put(SUCCESS_TASK, CNE_FILE_DTO);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/cne.xml"))
                .thenReturn(getClass().getResourceAsStream("/services/cne.xml"));

        zipAndUploadService.zipCnesAndSendToOutputs(TARGET_FOLDER,
                cnes,
                LOCAL_DATE,
                1);
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));

        assertFalse(new File("/tmp/cnes_out/2023-08-04").exists());
    }

    @Test
    void testZipCnesNotPresentAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> cnes = new HashMap<>();
        cnes.put(SUCCESS_TASK_NOT_PRESENT_STATUS, CGM_FILE_DTO_NOT_PRESENT);
        zipAndUploadService.zipCnesAndSendToOutputs(TARGET_FOLDER,
                cnes,
                LOCAL_DATE,
                1);
        //In case of NOT ProcessFileStatus == VALIDATED, should not retrieve cgm from minio
        verify(minioAdapterMock, never()).getFileFromFullPath(anyString());
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
    }

    // ------------ RAO_RESULT ------------
    @Test
    void testZipRaoResultAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> raoResults = new HashMap<>();
        raoResults.put(SUCCESS_TASK, RAO_RESULT_FILE_DTO);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/raoResult.json"))
                .thenReturn(getClass().getResourceAsStream("/services/raoResult.json"));

        zipAndUploadService.zipRaoResultsAndSendToOutputs(TARGET_FOLDER,
                raoResults,
                LOCAL_DATE);
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));

        assertFalse(new File("/tmp/raoResults_out/2023-08-04").exists());
    }

    @Test
    void testZipRaoResultNotPresentAndSendToOutputs() {
        final Map<TaskDto, ProcessFileDto> cnes = new HashMap<>();
        cnes.put(SUCCESS_TASK_NOT_PRESENT_STATUS, RAO_RESULT_FILE_DTO_NOT_PRESENT);
        zipAndUploadService.zipRaoResultsAndSendToOutputs(TARGET_FOLDER,
                cnes,
                LOCAL_DATE);
        //In case of NOT ProcessFileStatus == VALIDATED, should not retrieve cgm from minio
        verify(minioAdapterMock, never()).getFileFromFullPath(anyString());
        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
    }
    // ------------ UPLOAD ------------

    @Test
    void testUploadF3O3ToMinio() throws JAXBException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final FlowBasedConstraintDocument document = new FlowBasedConstraintDocument();
        zipAndUploadService.uploadF303ToMinio(document, TARGET_FOLDER, LOCAL_DATE, 1);
        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final FlowBasedConstraintDocument parsedDocument = parseInputStreamToObject(inputStreamArgumentCaptor.getValue(), FlowBasedConstraintDocument.class);
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230804-F303-01.xml", destinationPathArgumentCaptor.getValue());
        assertEquals(document, parsedDocument);
    }

    @Test
    void testUploadF3O5ToMinio() throws JAXBException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final ResponseMessageType responseMessage = new ResponseMessageType();
        zipAndUploadService.uploadF305ToMinio(TARGET_FOLDER, responseMessage, LOCAL_DATE, 1);
        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final ResponseMessageType parsedResponseMessage = parseInputStreamToObjectUsingJaxbElement(inputStreamArgumentCaptor.getValue(), ResponseMessageType.class);
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230804-F305-01.xml", destinationPathArgumentCaptor.getValue());
        assertEquals(responseMessage.getHeader(), parsedResponseMessage.getHeader());
        assertEquals(responseMessage.getReply(), parsedResponseMessage.getReply());
        assertEquals(responseMessage.getPayload(), parsedResponseMessage.getPayload());
    }

    @Test
    void testUploadF341ToMinio() throws IOException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final byte[] byteArray = new byte[1024];
        final RaoMetadata raoMetadata = new RaoMetadata();
        final String instantString = "2023-08-04T12:42:00Z";
        raoMetadata.setRaoRequestInstant(instantString);
        raoMetadata.setVersion(1);
        zipAndUploadService.uploadF341ToMinio(TARGET_FOLDER, byteArray, raoMetadata, 1);
        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final byte[] parsedResponseMessage = inputStreamToByteArray(inputStreamArgumentCaptor.getValue());
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230804-F341-01.csv", destinationPathArgumentCaptor.getValue());
        assertArrayEquals(byteArray, parsedResponseMessage);
    }

    private static byte[] inputStreamToByteArray(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            byteArrayOutputStream.write(buffer, 0, bytesRead);
        }
        return byteArrayOutputStream.toByteArray();
    }

    //Parsing of byte array
    private static <T> T parseInputStreamToObject(final InputStream inputStream,
                                                  final Class<T> classRef) throws JAXBException {
        final JAXBContext jaxbContext = JAXBContext.newInstance(classRef);
        final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return classRef.cast(unmarshaller.unmarshal(inputStream));
    }

    private static <T> T parseInputStreamToObjectUsingJaxbElement(final InputStream inputStream,
                                                                  final Class<T> classRef) throws JAXBException {
        final JAXBContext jaxbContext = JAXBContext.newInstance(classRef);
        final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

        // Deserialize using JAXBElement
        final JAXBElement<T> jaxbElement = unmarshaller.unmarshal(new StreamSource(inputStream), classRef);

        return jaxbElement.getValue();
    }
}
