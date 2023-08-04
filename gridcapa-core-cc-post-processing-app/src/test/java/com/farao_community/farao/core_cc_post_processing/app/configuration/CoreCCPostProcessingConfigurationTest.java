/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class CoreCCPostProcessingConfigurationTest {

    @Test
    void testCoreCCPostProcessingConfiguration() {
        CoreCCPostProcessingConfiguration.ProcessProperties processProperties = new CoreCCPostProcessingConfiguration.ProcessProperties("test", "Europe/Brussels");
        CoreCCPostProcessingConfiguration.UrlProperties urlProperties = new CoreCCPostProcessingConfiguration.UrlProperties("task_manager_2023-08-04T14:30:00Z", "task_manager_20230804");
        CoreCCPostProcessingConfiguration coreCCPostProcessingConfiguration = new CoreCCPostProcessingConfiguration(urlProperties, processProperties);
        assertEquals("task_manager_2023-08-04T14:30:00Z", coreCCPostProcessingConfiguration.getUrl().getTaskManagerTimestampUrl());
        assertEquals("task_manager_20230804", coreCCPostProcessingConfiguration.getUrl().getTaskManagerBusinessDateUrl());
        assertEquals("test", coreCCPostProcessingConfiguration.getProcess().getTag());
        assertEquals("Europe/Brussels", coreCCPostProcessingConfiguration.getProcess().getTimezone());
    }
}
