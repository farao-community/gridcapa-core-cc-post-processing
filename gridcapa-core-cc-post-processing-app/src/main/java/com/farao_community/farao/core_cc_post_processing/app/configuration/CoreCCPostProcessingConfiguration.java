/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * @author Alexandre Montigny {@literal <alexandre.montigny at rte-france.com>}
 */
@ConfigurationProperties("core-cc-post-processing")
public class CoreCCPostProcessingConfiguration {
    private final UrlProperties url;
    private final ProcessProperties process;
    private final PiSaConfiguration pisa;

    public CoreCCPostProcessingConfiguration(UrlProperties url, ProcessProperties process, PiSaConfiguration pisa) {
        this.url = url;
        this.process = process;
        this.pisa = pisa;
    }

    public UrlProperties getUrl() {
        return url;
    }

    public ProcessProperties getProcess() {
        return process;
    }

    public PiSaConfiguration getPisa() {
        return pisa;
    }

    public record UrlProperties(String taskManagerTimestampUrl, String taskManagerBusinessDateUrl) {
    }

    public record ProcessProperties(String tag, String timezone) {
    }

    @Bean(name = "piSaLink1Configuration")
    public PiSaConfiguration.PiSaLinkConfiguration getPiSaLink1Configuration() {
        return new PiSaConfiguration.PiSaLinkConfiguration(
            pisa.link1().nodeFr(),
            pisa.link1().nodeIt(),
            pisa.link1().fictiveLines(),
            pisa.link1().praName()
        );
    }

    @Bean(name = "piSaLink2Configuration")
    public PiSaConfiguration.PiSaLinkConfiguration getPiSaLink2Configuration() {
        return new PiSaConfiguration.PiSaLinkConfiguration(
            pisa.link2().nodeFr(),
            pisa.link2().nodeIt(),
            pisa.link2().fictiveLines(),
            pisa.link2().praName()
        );
    }
}
