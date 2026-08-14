/*
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.configuration;

import java.util.List;

/**
 * @author Joris Mancini {@literal <joris.mancini at rte-france.com>}
 */
public record PiSaConfiguration(List<String> alignedRaNames,
                                PisaLinkProperties link1,
                                PisaLinkProperties link2) {

    public record PisaLinkProperties(String nodeFr,
                                     String nodeIt,
                                     List<String> fictiveLines,
                                     String praName) {
    }

    public record PiSaLinkConfiguration(String piSaLinkFictiveNodeFr,
                                        String piSaLinkFictiveNodeIt,
                                        List<String> piSaLinkFictiveLines,
                                        String piSaLinkPraName) {
    }
}
