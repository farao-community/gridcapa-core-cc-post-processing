/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.core_cc_post_processing.app.configuration.CoreCCPostProcessingConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * @author Ameni Walha {@literal <ameni.walha at rte-france.com>}
 */
@EnableConfigurationProperties({CoreCCPostProcessingConfiguration.class})
@EnableWebMvc
@SuppressWarnings("hideutilityclassconstructor")
@SpringBootApplication
public class CoreCCPostProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(CoreCCPostProcessingApplication.class, args);
    }
}
