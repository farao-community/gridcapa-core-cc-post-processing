package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.farao_community.farao.minio_adapter.starter.MinioAdapterProperties;
import io.minio.MinioClient;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

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
