package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import com.farao_community.farao.core_cc_post_processing.app.util.JaxbUtil;
import com.farao_community.farao.core_cc_post_processing.app.util.NamingRules;
import com.farao_community.farao.core_cc_post_processing.app.util.RaoMetadata;
import com.farao_community.farao.core_cc_post_processing.app.util.ZipUtil;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.craccreation.creator.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.LocalDate;
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

    /**
     * F342 : zipped logs
     *
     * @param logList
     * @param logFileName
     */
    public void zipAndUploadLogs(final List<byte[]> logList,
                                 final String logFileName) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (final byte[] bytes : logList) {
                ZipUtil.collectAndZip(zos, bytes);
            }
            zos.close();
            // upload zipped result
            uploadOrThrow(baos.toByteArray(), logFileName, "Error while unzipping logs");
        } catch (final IOException e) {
            throw new CoreCCPostProcessingInternalException("Error while unzipping logs", e);
        }
    }

    /**
     * F304 : cgms
     *
     * @param targetMinioFolder
     * @param cgms
     * @param localDate
     * @param correlationId
     * @param timeInterval
     */
    public void zipCgmsAndSendToOutputs(final String targetMinioFolder,
                                        final Map<TaskDto, ProcessFileDto> cgms,
                                        final LocalDate localDate,
                                        final String correlationId,
                                        final String timeInterval) {
        final String cgmZipTmpDir = TMP + "cgms_out/" + localDate.toString() + "/";
        // add cgm xml header to tmp folder
        F305XmlGenerator.generateCgmXmlHeaderFile(cgms.keySet(), cgmZipTmpDir, localDate, correlationId, timeInterval);

        // Add all cgms from minio to tmp folder
        cgms.values()
                .stream()
                .filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED))
                .forEach(cgm -> {
                    final InputStream inputStream = minioAdapter.getFileFromFullPath(cgm.getFilePath());
                    final File cgmFile = new File(cgmZipTmpDir + cgm.getFilename());
                    try {
                        FileUtils.copyInputStreamToFile(inputStream, cgmFile);
                    } catch (final IOException e) {
                        throw new CoreCCPostProcessingInternalException("error while copying cgm to tmp folder", e);
                    }
                });

        // Zip tmp folder
        final byte[] cgmsZipResult = ZipUtil.zipDirectory(cgmZipTmpDir);
        final String targetCgmsFolderName = NamingRules.generateCgmZipName(localDate);
        final String targetCgmsFolderPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, targetCgmsFolderName);

        try {
            uploadOrThrow(cgmsZipResult, targetCgmsFolderPath, String.format("Exception occurred while zipping CGMs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(cgmZipTmpDir)); //NOSONAR
        }
    }

    /**
     * F299 : cnes
     *
     * @param targetMinioFolder
     * @param cnes
     * @param localDate
     */
    public void zipCnesAndSendToOutputs(final String targetMinioFolder,
                                        final Map<TaskDto, ProcessFileDto> cnes,
                                        final LocalDate localDate) {
        final String cneZipTmpDir = TMP + "cnes_out/" + localDate.toString() + "/";

        // Add all cnes from minio to tmp folder
        cnes.values().stream()
                .filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED))
                .forEach(cne -> {
                    final InputStream inputStream = minioAdapter.getFileFromFullPath(cne.getFilePath());
                    final File cneFile = new File(cneZipTmpDir + cne.getFilename());
                    try {
                        FileUtils.copyInputStreamToFile(inputStream, cneFile);
                    } catch (final IOException e) {
                        throw new CoreCCPostProcessingInternalException("error while copying cne to tmp folder", e);
                    }
                });

        final byte[] cneZipResult = ZipUtil.zipDirectory(cneZipTmpDir);
        final String targetCneFolderName = NamingRules.generateCneZipName(localDate);
        final String targetCneFolderPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, targetCneFolderName);

        try {
            uploadOrThrow(cneZipResult, targetCneFolderPath, String.format("Exception occurred while zipping CNEs of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(cneZipTmpDir)); //NOSONAR
        }
    }


    public void zipRaoResultsAndSendToOutputs(final String targetMinioFolder,
                                              final Map<TaskDto, ProcessFileDto> raoResults,
                                              final LocalDate localDate) {
        final String raoResultZipTmpDir = TMP + "raoResults_out/" + localDate.toString() + "/";

        // Add all raoResult json files from minio to tmp folder
        raoResults.values()
                .stream()
                .filter(processFileDto -> processFileDto.getProcessFileStatus().equals(ProcessFileStatus.VALIDATED))
                .forEach(raoResult -> {
                    try (final InputStream inputStream = minioAdapter.getFileFromFullPath(raoResult.getFilePath())) {
                        final File raoResultFile = new File(raoResultZipTmpDir + raoResult.getFilename());
                        FileUtils.copyInputStreamToFile(inputStream, raoResultFile);
                    } catch (final IOException e) {
                        throw new CoreCCPostProcessingInternalException("error while copying cgm to tmp folder", e);
                    }
                });

        // Zip tmp folder
        final byte[] raoResultZipResult = ZipUtil.zipDirectory(raoResultZipTmpDir);
        final String targetRaoResultZipName = NamingRules.generateRaoResultFilename(localDate);
        final String targetRaoResultZipPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, targetRaoResultZipName);

        try {
            uploadOrThrow(raoResultZipResult, targetRaoResultZipPath, String.format("Exception occurred while zipping RaoResults of business day %s", localDate));
        } finally {
            ZipUtil.deletePath(Paths.get(raoResultZipTmpDir)); //NOSONAR
        }

    }
    // --------- UPLOAD ---------

    /**
     * F303 : flowBasedConstraintDocument
     *
     * @param dailyFbDocument
     * @param targetMinioFolder
     * @param localDate
     */
    public void uploadF303ToMinio(final FlowBasedConstraintDocument dailyFbDocument,
                                  final String targetMinioFolder,
                                  final LocalDate localDate) {
        final byte[] dailyFbConstraint = JaxbUtil.writeInBytes(FlowBasedConstraintDocument.class, dailyFbDocument);
        final String fbConstraintFileName = NamingRules.generateOptimizedCbFileName(localDate);
        final String fbConstraintDestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, fbConstraintFileName);
        uploadOrThrow(dailyFbConstraint, fbConstraintDestinationPath, String.format("Exception occurred while uploading F303 file of business day %s", localDate));
    }

    /**
     * F305 : RaoResponse
     *
     * @param targetMinioFolder
     * @param responseMessage
     * @param localDate
     */
    public void uploadF305ToMinio(final String targetMinioFolder,
                                  final ResponseMessageType responseMessage,
                                  final LocalDate localDate) {
        final byte[] responseMessageBytes = JaxbUtil.marshallMessageAndSetJaxbProperties(responseMessage);
        final String f305FileName = NamingRules.generateRF305FileName(localDate);
        final String f305DestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, f305FileName);
        uploadOrThrow(responseMessageBytes, f305DestinationPath, String.format("Exception occurred while uploading F305 for business date %s", localDate));
    }

    public void uploadF341ToMinio(final String targetMinioFolder,
                                  final byte[] csv,
                                  final RaoMetadata raoMetadata) {
        final String metadataFileName = NamingRules.generateMetadataFileName(raoMetadata.getRaoRequestInstant(), raoMetadata.getVersion());
        final String metadataDestinationPath = NamingRules.generateOutputsDestinationPath(targetMinioFolder, metadataFileName);
        try (final InputStream csvIs = new ByteArrayInputStream(csv)) {
            minioAdapter.uploadOutput(metadataDestinationPath, csvIs);
        } catch (final IOException e) {
            throw new CoreCCInternalException("Exception occurred while uploading metadata file", e);
        }
    }

    private void uploadOrThrow(final byte[] byteArray,
                               final String destinationPath,
                               final String message) {
        try (final InputStream inputStream = new ByteArrayInputStream(byteArray)) {
            minioAdapter.uploadOutput(destinationPath, inputStream);
        } catch (final IOException e) {
            throw new CoreCCPostProcessingInternalException(message, e);
        }
    }
}
