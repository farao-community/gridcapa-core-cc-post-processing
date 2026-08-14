/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.NamingRules;
import com.farao_community.farao.core_cc_post_processing.app.util.ZipUtil;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

@Service
public class ZipAndUploadService {

    private final MinioAdapter minioAdapter;
    private static final String TMP = "/tmp/";

    public ZipAndUploadService(final MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    // --------- ZIP & UPLOAD ---------

    public void zipAndUploadLogs(final String targetMinioFolder,
                                 final List<byte[]> logsList,
                                 final String businessDate,
                                 final int version) {
        final byte[] zipResult;
        final String zipFilename = NamingRules.generateLogsZipFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, zipFilename);
        final String errorMessage = String.format("Exception occurred while zipping logs of business day %s", businessDate);

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ZipOutputStream zos = new ZipOutputStream(baos)) {

            logsList.forEach(logsBytes -> ZipUtil.collectAndZip(zos, logsBytes));
            zos.close(); // NOSONAR because the `zos` ZipOutputStream must be closed before calling `toByteArray()` method on `baos`
            zipResult = baos.toByteArray();
        } catch (final IOException e) {
            throw new CoreCCPostProcessingInternalException(errorMessage, e);
        }

        uploadOrThrow(zipResult, destinationPath, errorMessage);
    }

    public void zipCgmsAndSendToOutputs(final String targetMinioFolder,
                                        final Map<TaskDto, ProcessFileDto> cgms,
                                        final LocalDate businessDate,
                                        final String correlationId,
                                        final String timeInterval,
                                        final int version) {
        final String zipTmpDir = TMP + "cgms_out/" + businessDate.toString() + "/";
        XmlGenerator.generateCgmXmlHeaderFile(cgms.keySet(), zipTmpDir, businessDate, correlationId, timeInterval);
        copyFilesToTmpDir(cgms.values(), zipTmpDir, "CGM");

        final byte[] zipResult = ZipUtil.zipDirectory(zipTmpDir);
        final String zipFilename = NamingRules.generateCgmZipFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, zipFilename);

        try {
            final String errorMessage = String.format("Exception occurred while zipping CGMs of business day %s", businessDate);
            uploadOrThrow(zipResult, destinationPath, errorMessage);
        } finally {
            ZipUtil.deletePath(Paths.get(zipTmpDir)); //NOSONAR
        }
    }

    public void zipCnesAndSendToOutputs(final String targetMinioFolder,
                                        final Map<TaskDto, ProcessFileDto> cnes,
                                        final LocalDate businessDate,
                                        final int version) {
        final String zipTmpDir = TMP + "cnes_out/" + businessDate.toString() + "/";
        copyFilesToTmpDir(cnes.values(), zipTmpDir, "CNE");

        final byte[] zipResult = ZipUtil.zipDirectory(zipTmpDir);
        final String zipFilename = NamingRules.generateCneZipFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, zipFilename);

        try {
            final String errorMessage = String.format("Exception occurred while zipping CNEs of business day %s", businessDate);
            uploadOrThrow(zipResult, destinationPath, errorMessage);
        } finally {
            ZipUtil.deletePath(Paths.get(zipTmpDir)); //NOSONAR
        }
    }

    public void zipRaoResultsAndSendToOutputs(final String targetMinioFolder,
                                              final Map<TaskDto, ProcessFileDto> raoResults,
                                              final LocalDate businessDate) {
        final String zipTmpDir = TMP + "raoResults_out/" + businessDate.toString() + "/";
        copyFilesToTmpDir(raoResults.values(), zipTmpDir, "RaoResult");

        final byte[] zipResult = ZipUtil.zipDirectory(zipTmpDir);
        final String zipFilename = NamingRules.generateRaoResultZipFilename(businessDate);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, zipFilename);

        try {
            final String errorMessage = String.format("Exception occurred while zipping RaoResults of business day %s", businessDate);
            uploadOrThrow(zipResult, destinationPath, errorMessage);
        } finally {
            ZipUtil.deletePath(Paths.get(zipTmpDir)); //NOSONAR
        }
    }

    private void copyFilesToTmpDir(final Collection<ProcessFileDto> fileDtos,
                                   final String tmpDir,
                                   final String filetype) {
        fileDtos.forEach(fileDto -> {
            try (final InputStream inputStream = minioAdapter.getFileFromFullPath(fileDto.getFilePath())) {
                final File file = new File(tmpDir + fileDto.getFilename());
                FileUtils.copyInputStreamToFile(inputStream, file);
            } catch (final IOException e) {
                final String errorMessage = String.format("Exception occurred while copying %s %s to tmp folder", filetype, fileDto.getFilename());
                throw new CoreCCPostProcessingInternalException(errorMessage, e);
            }
        });
    }

    // --------- SIMPLE UPLOAD ---------

    public void uploadCbcoraToMinio(final String targetMinioFolder,
                                    final FlowBasedConstraintDocument fbConstraintDocument,
                                    final LocalDate businessDate,
                                    final int version) {
        final byte[] fbConstraintBytes = JaxbUtil.writeInBytes(FlowBasedConstraintDocument.class, fbConstraintDocument);
        final String filename = NamingRules.generateCbcoraFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, filename);
        final String errorMessage = String.format("Exception occurred while uploading CBCORA file of business date %s", businessDate);
        uploadOrThrow(fbConstraintBytes, destinationPath, errorMessage);
    }

    public void uploadRaoResponseToMinio(final String targetMinioFolder,
                                         final ResponseMessageType raoResponse,
                                         final LocalDate businessDate,
                                         final int version) {
        final byte[] raoResponseBytes = JaxbUtil.marshallMessageAndSetJaxbProperties(raoResponse);
        final String filename = NamingRules.generateRaoResponseFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, filename);
        final String errorMessage = String.format("Exception occurred while uploading RAO response for business date %s", businessDate);
        uploadOrThrow(raoResponseBytes, destinationPath, errorMessage);
    }

    public void uploadMetadataToMinio(final String targetMinioFolder,
                                      final byte[] metadataCsvBytes,
                                      final String businessDate,
                                      final int version) {
        final String filename = NamingRules.generateMetadataFilename(businessDate, version);
        final String destinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, filename);
        final String errorMessage = String.format("Exception occurred while uploading metadata file for business date %s", businessDate);
        uploadOrThrow(metadataCsvBytes, destinationPath, errorMessage);
    }

    private void uploadOrThrow(final byte[] byteArray,
                               final String destinationPath,
                               final String errorMessage) {
        try (final InputStream inputStream = new ByteArrayInputStream(byteArray)) {
            minioAdapter.uploadOutput(destinationPath, inputStream);
        } catch (final IOException e) {
            throw new CoreCCPostProcessingInternalException(errorMessage, e);
        }
    }
}
