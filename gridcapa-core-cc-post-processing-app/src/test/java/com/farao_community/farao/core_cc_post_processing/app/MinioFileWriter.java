/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.farao_community.farao.minio_adapter.starter.MinioAdapterProperties;
import io.minio.MinioClient;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class MinioFileWriter extends MinioAdapter {

    public MinioFileWriter(MinioAdapterProperties properties, MinioClient minioClient) {
        super(properties, minioClient);
    }

    @Override
    public void uploadOutput(String path, InputStream inputStream) {
        File tmpDir = new File("/tmp/outputs/");
        if (!tmpDir.exists()) {
            boolean created = tmpDir.mkdir();
        }
        if (path.startsWith("RAO_OUTPUTS_DIR")) {
            path = "/tmp/outputs/" + path;
        }
        File targetFile = new File(path);
        try {
            FileUtils.copyInputStreamToFile(inputStream, targetFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getFile(String path) {
        return getClass().getResourceAsStream("/services/" + path);
    }
}
