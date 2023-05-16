/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class ZipUtil {

    private static final int BUFFER_SIZE = 4096;
    private static final double THRESHOLD_RATIO = 100;
    private static final int THRESHOLD_ENTRIES = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZipUtil.class);

    private ZipUtil() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static void unzipFile(Path zipFilePath, Path destDirectory) {
        if (!destDirectory.toFile().exists() && !destDirectory.toFile().mkdir()) {
            LOGGER.error("Cannot create destination directory '{}'", destDirectory);
            throw new CoreCCPostProcessingInternalException(String.format("Cannot create destination directory '%s'", destDirectory));
        }
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath.toFile()))) {
            ZipEntry entry = zipIn.getNextEntry(); //NOSONAR
            int totalEntries = 0;
            // iterates over entries in the zip file
            while (entry != null) {
                totalEntries++;
                Path filePath = Path.of(destDirectory + File.separator + entry.getName()).normalize();
                if (!filePath.startsWith(destDirectory)) {
                    throw new IOException("Entry is outside of the target directory");
                }
                if (!entry.isDirectory()) {
                    // if the entry is a file, extracts it
                    extractFile(zipIn, filePath.toString(), entry.getCompressedSize());
                } else {
                    // if the entry is a directory, make the directory
                    File dir = new File(filePath.toString()); //NOSONAR
                    dir.mkdir();
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry(); //NOSONAR
                if (totalEntries > THRESHOLD_ENTRIES) {
                    throw new IOException("Entry threshold reached while unzipping.");
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error while extracting file '{}'", zipFilePath.getFileName());
            throw new CoreCCPostProcessingInternalException(String.format("Error while extracting file '%s'", zipFilePath.getFileName()), e);
        }
    }

    private static void extractFile(ZipInputStream zipIn, String filePath, long compressedSize) throws IOException {
        float totalSizeEntry = 0;
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) { //NOSONAR
            byte[] bytesIn = new byte[BUFFER_SIZE];
            int read;
            while ((read = zipIn.read(bytesIn)) != -1) {
                bos.write(bytesIn, 0, read);
                totalSizeEntry += read;
                double compressionRatio = totalSizeEntry / compressedSize;
                if (compressionRatio > THRESHOLD_RATIO) {
                    throw new IOException("Ratio between compressed and uncompressed data suspiciously large.");
                }
            }
        }
    }

    public static void deletePath(Path path) {
        try {
            FileSystemUtils.deleteRecursively(path);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred while trying to delete recursively from path: " + path.toString());
        }
    }

    public static byte[] zipDirectory(String inputDirectory) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(os)) {
            recursiveZip(inputDirectory, zos, inputDirectory);
            zos.close();
            return os.toByteArray();
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while compressing directory '%s'", inputDirectory), e);
        }
    }

    private static void recursiveZip(String dir2zip, ZipOutputStream zos, String referencePath) {
        //create a new File object based on the directory we have to zip
        File zipDir = new File(dir2zip); //NOSONAR
        //get a listing of the directory content
        String[] dirList = zipDir.list();
        byte[] readBuffer = new byte[2156];
        int bytesIn;
        //loop through dirList, and zip the files
        for (String fileOrDir : dirList) {
            Path path = Path.of(zipDir.getPath(), fileOrDir).normalize();
            if (!path.startsWith(zipDir.getPath())) {
                continue;
            }
            File f = new File(zipDir, fileOrDir); //NOSONAR
            if (f.isDirectory()) {
                //if the File object is a directory, call this
                //function again to add its content recursively
                String filePath = f.getPath();
                recursiveZip(filePath, zos, referencePath);
                //loop again
                continue;
            }
            //if we reached here, the File object f was not a directory
            //create a FileInputStream on top of f
            try (FileInputStream fis = new FileInputStream(f)) {
                // create a new zip entry
                String fileRelativePath = Paths.get(referencePath).relativize(Paths.get(f.getPath())).toString(); //NOSONAR
                ZipEntry anEntry = new ZipEntry(fileRelativePath);
                //place the zip entry in the ZipOutputStream object
                zos.putNextEntry(anEntry);
                //now write the content of the file to the ZipOutputStream
                while ((bytesIn = fis.read(readBuffer)) != -1) {
                    zos.write(readBuffer, 0, bytesIn);
                }
            } catch (IOException e) {
                throw new CoreCCPostProcessingInternalException(e.getMessage(), e);
            }
        }
    }

}

