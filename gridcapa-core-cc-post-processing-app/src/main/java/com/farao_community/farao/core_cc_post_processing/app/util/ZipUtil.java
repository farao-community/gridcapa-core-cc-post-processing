/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.FileSystemUtils;

import java.io.*;
import java.nio.file.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public final class ZipUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZipUtil.class);

    private ZipUtil() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    public static void deletePath(Path path) {
        try {
            FileSystemUtils.deleteRecursively(path);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred while trying to delete recursively from path: " + path.toString(), e);
        }
    }

    public static byte[] zipDirectory(String inputDirectory) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(os)) {
            recursiveZip(inputDirectory, zos, inputDirectory);
            zos.close(); // NOSONAR because the `zos` ZipOutputStream must be closed before calling `toByteArray()` method on `os`
            return os.toByteArray();
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while compressing directory '%s'", inputDirectory), e);
        }
    }

    private static void recursiveZip(String dir2zip, ZipOutputStream zos, String referencePath) {
        //create a new File object based on the directory we have to zip
        File zipDir = new File(dir2zip); //NOSONAR
        Paths.get(dir2zip);
        //get a listing of the directory content
        String[] dirList = zipDir.list();
        if (ArrayUtils.isEmpty(dirList)) {
            return;
        }
        byte[] readBuffer = new byte[2156];
        //loop through dirList, and zip the files
        for (String fileOrDir : dirList) {
            Path path = Path.of(zipDir.getPath(), fileOrDir).normalize();
            if (path.startsWith(zipDir.getPath())) {
                File f = new File(zipDir, fileOrDir); //NOSONAR
                if (f.isDirectory()) {
                    //if the File object is a directory, call this
                    //function again to add its content recursively
                    String filePath = f.getPath();
                    recursiveZip(filePath, zos, referencePath);
                } else {
                    //if we reached here, the File object f was not a directory
                    addFileToZip(zos, referencePath, f, readBuffer);
                }
            }
        }
    }

    private static void addFileToZip(final ZipOutputStream zos, final String referencePath, final File file, final byte[] readBuffer) {
        int bytesIn;
        //create a FileInputStream on top of file
        try (FileInputStream fis = new FileInputStream(file)) {
            // create a new zip entry
            String fileRelativePath = Paths.get(referencePath).relativize(Paths.get(file.getPath())).toString(); //NOSONAR
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

    public static void collectAndZip(ZipOutputStream zos, byte[] bytes) {
        byte[] byteBuff = new byte[1024];
        try (ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(bytes))) {
            ZipEntry entry = zipIn.getNextEntry(); //NOSONAR
            int totalEntries = 0;
            // iterates over entries in the zip file
            while (entry != null) {
                totalEntries++;
                if (!entry.isDirectory()) {
                    // if the entry is a file
                    zos.putNextEntry(entry);
                    for (int bytesRead; (bytesRead = zipIn.read(byteBuff)) != -1; ) {
                        zos.write(byteBuff, 0, bytesRead);
                    }
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry(); //NOSONAR
                if (totalEntries > 10000) {
                    throw new IOException("Entry threshold reached while unzipping.");
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error while unzipping logs");
            throw new CoreCCPostProcessingInternalException("Error while unzipping logs", e);
        }
    }

}

