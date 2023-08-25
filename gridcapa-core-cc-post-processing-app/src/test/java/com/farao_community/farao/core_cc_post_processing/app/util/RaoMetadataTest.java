/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class RaoMetadataTest {

    @Test
    void generateOverallStatus() {
        assertEquals("FAILURE", RaoMetadata.generateOverallStatus(Set.of("PENDING", "RUNNING", "FAILURE", "SUCCESS")));
        assertEquals("PENDING", RaoMetadata.generateOverallStatus(Set.of("RUNNING", "PENDING", "SUCCESS")));
        assertEquals("RUNNING", RaoMetadata.generateOverallStatus(Set.of("RUNNING", "SUCCESS")));
        assertEquals("SUCCESS", RaoMetadata.generateOverallStatus(Set.of("SUCCESS")));
        assertEquals("SUCCESS", RaoMetadata.generateOverallStatus(Set.of()));
        CoreCCPostProcessingInternalException exception = assertThrows(CoreCCPostProcessingInternalException.class, () -> RaoMetadata.generateOverallStatus(Set.of("SUCCESS", "ERROR")));
        assertEquals("Invalid overall status", exception.getMessage());
    }
}
