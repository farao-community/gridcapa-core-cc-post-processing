/*
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.util.FileUtils;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * @author Ameni Walha {@literal <ameni.walha at rte-france.com>}
 */
@SpringBootTest
class CoreCCPostProcessingHandlerTest {

    @Autowired
    private CoreCCPostProcessingHandler coreCCPostProcessingHandler;

    @MockBean
    private RestTemplateBuilder restTemplateBuilder;
    @MockBean
    private FileUtils fileUtils;
    @MockBean
    private MinioAdapter minioAdapter;

    @Test
    void publishProcessTaskManagerError() {
        RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
        Mockito.when(restTemplateBuilder.build()).thenReturn(restTemplate);
        ResponseEntity responseEntity = Mockito.mock(ResponseEntity.class);
        Mockito.when(restTemplate.getForEntity("http://mockUrl/2022-11-22", TaskDto[].class)).thenReturn(responseEntity);
        Mockito.when(responseEntity.getStatusCode()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
