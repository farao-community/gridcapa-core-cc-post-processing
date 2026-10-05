/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class MetadataUtilTest {

    @Test
    void generateOverallStatus() {
        final CoreCCMetadata pending = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(pending.getStatus()).thenReturn("PENDING");
        final CoreCCMetadata running = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(running.getStatus()).thenReturn("RUNNING");
        final CoreCCMetadata failure = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(failure.getStatus()).thenReturn("FAILURE");
        final CoreCCMetadata success = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(success.getStatus()).thenReturn("SUCCESS");
        final CoreCCMetadata error = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(error.getStatus()).thenReturn("ERROR");

        assertEquals("FAILURE", MetadataUtil.generateOverallStatus(Set.of(pending, running, failure, success)));
        assertEquals("PENDING", MetadataUtil.generateOverallStatus(Set.of(running, pending, success)));

        final Set<CoreCCMetadata> runningSuccess = Set.of(running, success);
        assertThrows(CoreCCInternalException.class, () -> MetadataUtil.generateOverallStatus(runningSuccess));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of(success)));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of()));

        final Set<CoreCCMetadata> successError = Set.of(success, error);
        CoreCCPostProcessingInternalException exception = assertThrows(CoreCCPostProcessingInternalException.class, () -> MetadataUtil.generateOverallStatus(successError));
        assertEquals("Invalid overall status", exception.getMessage());
    }
}
