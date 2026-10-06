/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.entities.ComputationArea;
import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class MetadataUtilTest {
    private CoreCCMetadata pending;
    private CoreCCMetadata running;
    private CoreCCMetadata failure;
    private CoreCCMetadata success;
    private CoreCCMetadata error;

    @BeforeEach
    void setUp() {
        pending = Mockito.mock(CoreCCMetadata.class);
        running = Mockito.mock(CoreCCMetadata.class);
        failure = Mockito.mock(CoreCCMetadata.class);
        success = Mockito.mock(CoreCCMetadata.class);
        error = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(pending.getSemComputationStatus()).thenReturn("PENDING");
        Mockito.when(pending.getContinentalComputationStatus()).thenReturn("PENDING");
        Mockito.when(running.getSemComputationStatus()).thenReturn("RUNNING");
        Mockito.when(running.getContinentalComputationStatus()).thenReturn("RUNNING");
        Mockito.when(failure.getSemComputationStatus()).thenReturn("FAILURE");
        Mockito.when(failure.getContinentalComputationStatus()).thenReturn("FAILURE");
        Mockito.when(success.getSemComputationStatus()).thenReturn("SUCCESS");
        Mockito.when(success.getContinentalComputationStatus()).thenReturn("SUCCESS");
        Mockito.when(error.getSemComputationStatus()).thenReturn("ERROR");
        Mockito.when(error.getContinentalComputationStatus()).thenReturn("ERROR");
    }

    @Test
    void generateOverallStatus() {
        assertEquals("FAILURE", MetadataUtil.generateOverallStatus(Set.of(pending, running, failure, success), ComputationArea.ALL));
        assertEquals("PENDING", MetadataUtil.generateOverallStatus(Set.of(running, pending, success), ComputationArea.ALL));

        final Set<CoreCCMetadata> runningSuccess = Set.of(running, success);
        assertThrows(CoreCCPostProcessingInternalException.class, () -> MetadataUtil.generateOverallStatus(runningSuccess, ComputationArea.ALL));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.ALL));
        assertEquals("SUCCESS", MetadataUtil.generateOverallStatus(Set.of(), ComputationArea.ALL));

        final Set<CoreCCMetadata> successError = Set.of(success, error);
        CoreCCPostProcessingInternalException exception = assertThrows(CoreCCPostProcessingInternalException.class, () -> MetadataUtil.generateOverallStatus(successError, ComputationArea.ALL));
        assertEquals("Invalid overall status", exception.getMessage());
    }
}
