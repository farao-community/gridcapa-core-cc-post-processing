/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.configuration.CoreCCPostProcessingConfiguration;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.web.client.RestTemplateBuilder;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class CoreCCPostProcessingHandlerTest {
    private final CoreCCPostProcessingConfiguration.UrlProperties url = new CoreCCPostProcessingConfiguration.UrlProperties("/timestampUrl", "/businessDateUrl");
    private final CoreCCPostProcessingConfiguration.ProcessProperties properties = new CoreCCPostProcessingConfiguration.ProcessProperties("tag", "Europe/Brussels");
    private final CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration = new CoreCCPostProcessingConfiguration(url, properties);
    private final RestTemplateBuilder restTemplateBuilder = Mockito.mock(RestTemplateBuilder.class);
    private final PostProcessingService postProcessingService = Mockito.mock(PostProcessingService.class);
    private CoreCCPostProcessingHandler postProcessingHandler;

    @BeforeEach
    void setUp() {
        postProcessingHandler = new CoreCCPostProcessingHandler(coreCCPostProcessingConfiguration, restTemplateBuilder, postProcessingService);
    }

    @Test
    void getLogsForTaskWithError() {
        Mockito.doThrow(RuntimeException.class).when(restTemplateBuilder).build();
        Set<TaskDto> taskList = Set.of(Utils.SUCCESS_TASK);
        List<byte[]> logList = postProcessingHandler.getLogsForTask(taskList);
        assertEquals(0, logList.size());
    }
}
