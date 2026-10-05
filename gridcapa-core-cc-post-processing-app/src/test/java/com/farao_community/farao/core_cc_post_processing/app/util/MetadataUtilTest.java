/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class MetadataUtilTest {

    @Test
    void generateOverallStatus() {
        assertEquals("FAILURE", MetadataUtil.generateOverallStatus(Set.of("PENDING", "RUNNING", "FAILURE", "SUCCESS")));
        assertEquals("PENDING", MetadataUtil.generateOverallStatus(Set.of("RUNNING", "PENDING", "SUCCESS")));
        final Set<String> runningSuccess = Set.of("RUNNING", "SUCCESS");
        assertThrows(CoreCCInternalException.class, () -> MetadataUtil.generateOverallStatus(runningSuccess));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of("SUCCESS")));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of()));
        final Set<String> successError = Set.of("SUCCESS", "ERROR");
        CoreCCPostProcessingInternalException exception = assertThrows(CoreCCPostProcessingInternalException.class, () -> MetadataUtil.generateOverallStatus(successError));
        assertEquals("Invalid overall status", exception.getMessage());
    }
}
