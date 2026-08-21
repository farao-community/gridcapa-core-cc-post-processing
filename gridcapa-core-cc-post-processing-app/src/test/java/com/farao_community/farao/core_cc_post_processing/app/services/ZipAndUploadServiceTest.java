/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.farao_community.farao.core_cc_post_processing.app.Utils.CGM_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.CNE_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.RAO_RESULT_FILE_DTO_2019;
import static com.farao_community.farao.core_cc_post_processing.app.Utils.SUCCESS_TASK_2019;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
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
    void zipAndUploadLogsTest() throws IOException {
        final List<byte[]> logList = List.of(fileToByteArray("/services/export/logs1.txt"));
        final String instantString = "2023-08-04T12:42:00Z";

        zipAndUploadService.zipAndUploadLogs(TARGET_FOLDER, logList, instantString, 1);

        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
    }

    @Test
    void zipAndUploadLogsThrowsExceptionTest() throws IOException {
        final List<byte[]> logList = List.of(fileToByteArray("/services/export/logs1.txt"));
        doThrow(CoreCCPostProcessingInternalException.class)
            .when(minioAdapterMock)
            .uploadOutput(anyString(), any(InputStream.class));
        final String instantString = "2023-08-04T12:42:00Z";

        Assertions.assertThrows(
            CoreCCPostProcessingInternalException.class,
            () -> zipAndUploadService.zipAndUploadLogs(TARGET_FOLDER, logList, instantString, 1)
        );
    }

    @Test
    void zipCgmsAndSendToOutputsTest() {
        final Map<TaskDto, ProcessFileDto> cgms = Map.of(SUCCESS_TASK_2019, CGM_FILE_DTO_2019);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/network.uct"))
            .thenReturn(getClass().getResourceAsStream("/services/network.uct"));
        zipAndUploadService.zipCgmsAndSendToOutputs(
            TARGET_FOLDER,
            cgms,
            LOCAL_DATE,
            "00000000-0000-0000-0000-000000000000",
            "2019-01-07T23:00Z/2019-01-08T23:00Z",
            1
        );

        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
        assertFalse(new File("/tmp/cgms_out/2023-08-04").exists());
    }

    @Test
    void zipCnesAndSendToOutputsTest() {
        final Map<TaskDto, ProcessFileDto> cnes = Map.of(SUCCESS_TASK_2019, CNE_FILE_DTO_2019);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/cne.xml"))
            .thenReturn(getClass().getResourceAsStream("/services/cne.xml"));

        zipAndUploadService.zipCnesAndSendToOutputs(TARGET_FOLDER, cnes, LOCAL_DATE, 1);

        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
        assertFalse(new File("/tmp/cnes_out/2023-08-04").exists());
    }

    @Test
    void zipRaoResultAndSendToOutputsTest() {
        final Map<TaskDto, ProcessFileDto> raoResults = Map.of(SUCCESS_TASK_2019, RAO_RESULT_FILE_DTO_2019);
        when(minioAdapterMock.getFileFromFullPath("/CORE/CC/raoResult.json"))
            .thenReturn(getClass().getResourceAsStream("/services/raoResult.json"));

        zipAndUploadService.zipRaoResultsAndSendToOutputs(TARGET_FOLDER, raoResults, LOCAL_DATE);

        verify(minioAdapterMock).uploadOutput(anyString(), any(InputStream.class));
        assertFalse(new File("/tmp/raoResults_out/2023-08-04").exists());
    }

    @Test
    void uploadCbcoraToMinioTest() throws JAXBException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final FlowBasedConstraintDocument document = new FlowBasedConstraintDocument();

        zipAndUploadService.uploadCbcoraToMinio(TARGET_FOLDER, document, LOCAL_DATE, 1);

        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final FlowBasedConstraintDocument parsedDocument = parseInputStreamToObject(inputStreamArgumentCaptor.getValue(), FlowBasedConstraintDocument.class);
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_20230804-F303-01.xml", destinationPathArgumentCaptor.getValue());
        assertEquals(document, parsedDocument);
    }

    @Test
    void uploadRaoResponseToMinioTest() throws JAXBException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final ResponseMessageType responseMessage = new ResponseMessageType();

        zipAndUploadService.uploadRaoResponseToMinio(TARGET_FOLDER, responseMessage, LOCAL_DATE, 1);

        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final ResponseMessageType parsedResponseMessage = parseInputStreamToObjectUsingJaxbElement(inputStreamArgumentCaptor.getValue(), ResponseMessageType.class);
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-305_20230804-F305-01.xml", destinationPathArgumentCaptor.getValue());
        assertEquals(responseMessage.getHeader(), parsedResponseMessage.getHeader());
        assertEquals(responseMessage.getReply(), parsedResponseMessage.getReply());
        assertEquals(responseMessage.getPayload(), parsedResponseMessage.getPayload());
    }

    @Test
    void uploadMetadataToMinioTest() throws IOException {
        final ArgumentCaptor<InputStream> inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<String> destinationPathArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final byte[] byteArray = "Dummy string".getBytes();
        final String instantString = "2023-08-04T12:42:00Z";

        zipAndUploadService.uploadMetadataToMinio(TARGET_FOLDER, byteArray, instantString, 1);

        verify(minioAdapterMock).uploadOutput(destinationPathArgumentCaptor.capture(), inputStreamArgumentCaptor.capture());
        final InputStream inputStream = inputStreamArgumentCaptor.getValue();
        final byte[] parsedResponseMessage = inputStream.readAllBytes();
        assertEquals("targetFolder/outputs/22XCORESO------S_10V1001C--00236Y_CORE-FB-341_20230804-F341-01.csv", destinationPathArgumentCaptor.getValue());
        assertArrayEquals(byteArray, parsedResponseMessage);
    }

    // Util methods

    private byte[] fileToByteArray(final String filename) throws IOException {
        try (final InputStream inputStream = getClass().getResourceAsStream(filename)) {
            return inputStream.readAllBytes();
        }
    }

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
