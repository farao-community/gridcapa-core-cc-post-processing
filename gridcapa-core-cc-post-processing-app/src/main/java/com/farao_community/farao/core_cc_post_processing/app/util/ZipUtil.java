/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

