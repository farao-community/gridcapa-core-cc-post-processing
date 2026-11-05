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
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class MetadataUtilTest {
    private CoreCCMetadata pending;
    private CoreCCMetadata failure;
    private CoreCCMetadata success;
    private CoreCCMetadata error;

    @BeforeEach
    void setUp() {
        pending = Mockito.mock(CoreCCMetadata.class);
        failure = Mockito.mock(CoreCCMetadata.class);
        success = Mockito.mock(CoreCCMetadata.class);
        error = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(pending.getSemComputationStatus()).thenReturn("PENDING");
        Mockito.when(pending.getContinentalComputationStatus()).thenReturn("PENDING");
        Mockito.when(failure.getSemComputationStatus()).thenReturn("FAILURE");
        Mockito.when(failure.getContinentalComputationStatus()).thenReturn("FAILURE");
        Mockito.when(success.getSemComputationStatus()).thenReturn("SUCCESS");
        Mockito.when(success.getContinentalComputationStatus()).thenReturn("SUCCESS");
        Mockito.when(error.getSemComputationStatus()).thenReturn("ERROR");
        Mockito.when(error.getContinentalComputationStatus()).thenReturn("ERROR");
    }

    @Test
    void computeStatusForTask() {
        final SoftAssertions assertions = new SoftAssertions();
        assertions.assertThat(MetadataUtil.computeStatusForTask()).isNull();
        assertions.assertThat(MetadataUtil.computeStatusForTask((String) null)).isNull();
        assertions.assertThat(MetadataUtil.computeStatusForTask("SUCCESS", null)).isEqualTo("SUCCESS");
        assertions.assertThat(MetadataUtil.computeStatusForTask(null, "SUCCESS")).isEqualTo("SUCCESS");
        assertions.assertThat(MetadataUtil.computeStatusForTask("SUCCESS", "SUCCESS")).isEqualTo("SUCCESS");
        assertions.assertThat(MetadataUtil.computeStatusForTask("PENDING", "SUCCESS")).isEqualTo("PARTIAL_SUCCESS");
        assertions.assertThat(MetadataUtil.computeStatusForTask("FAILURE", "SUCCESS")).isEqualTo("PARTIAL_SUCCESS");
        assertions.assertThat(MetadataUtil.computeStatusForTask("PENDING", "PENDING")).isEqualTo("PENDING");
        assertions.assertThat(MetadataUtil.computeStatusForTask("PENDING", "FAILURE")).isEqualTo("FAILURE");
        assertions.assertThat(MetadataUtil.computeStatusForTask("FAILURE", "FAILURE")).isEqualTo("FAILURE");
        assertions.assertAll();
    }

    @Test
    void generateOverallStatusNoStatus() {
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(), ComputationArea.CONTINENTAL)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(), ComputationArea.SEM)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(), ComputationArea.ALL)).isEqualTo("SUCCESS");
    }

    @Test
    void generateOverallStatusContinentalOnly() {
        final Set<CoreCCMetadata> successError = Set.of(success, error);
        Mockito.when(pending.getSemComputationStatus()).thenReturn(null);
        Mockito.when(failure.getSemComputationStatus()).thenReturn(null);
        Mockito.when(success.getSemComputationStatus()).thenReturn(null);
        Mockito.when(error.getSemComputationStatus()).thenReturn(null);

        Assertions.assertThatExceptionOfType(CoreCCPostProcessingInternalException.class)
            .isThrownBy(() -> MetadataUtil.generateOverallStatus(successError, ComputationArea.CONTINENTAL))
            .withMessage("Invalid overall status");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.CONTINENTAL)).isEqualTo("FAILURE");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.CONTINENTAL)).isEqualTo("PENDING");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.CONTINENTAL)).isEqualTo("SUCCESS");

        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success, error), ComputationArea.SEM)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.SEM)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.SEM)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.SEM)).isEmpty();

        Assertions.assertThatExceptionOfType(CoreCCPostProcessingInternalException.class)
            .isThrownBy(() -> MetadataUtil.generateOverallStatus(successError, ComputationArea.ALL))
            .withMessage("Invalid overall status");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.ALL)).isEqualTo("FAILURE");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.ALL)).isEqualTo("PENDING");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.ALL)).isEqualTo("SUCCESS");
    }

    @Test
    void generateOverallStatusSemOnly() {
        final Set<CoreCCMetadata> successError = Set.of(success, error);
        Mockito.when(pending.getContinentalComputationStatus()).thenReturn(null);
        Mockito.when(failure.getContinentalComputationStatus()).thenReturn(null);
        Mockito.when(success.getContinentalComputationStatus()).thenReturn(null);
        Mockito.when(error.getContinentalComputationStatus()).thenReturn(null);

        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success, error), ComputationArea.CONTINENTAL)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.CONTINENTAL)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.CONTINENTAL)).isEmpty();
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.CONTINENTAL)).isEmpty();

        Assertions.assertThatExceptionOfType(CoreCCPostProcessingInternalException.class)
            .isThrownBy(() -> MetadataUtil.generateOverallStatus(successError, ComputationArea.SEM))
            .withMessage("Invalid overall status");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.SEM)).isEqualTo("FAILURE");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.SEM)).isEqualTo("PENDING");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.SEM)).isEqualTo("SUCCESS");

        Assertions.assertThatExceptionOfType(CoreCCPostProcessingInternalException.class)
            .isThrownBy(() -> MetadataUtil.generateOverallStatus(successError, ComputationArea.ALL))
            .withMessage("Invalid overall status");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, failure, success), ComputationArea.ALL)).isEqualTo("FAILURE");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(pending, success), ComputationArea.ALL)).isEqualTo("PENDING");
        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(success), ComputationArea.ALL)).isEqualTo("SUCCESS");
    }

    @Test
    void generateOverallStatusWithDifferentContinentalAndSemStatuses() {
        final CoreCCMetadata bistatus = Mockito.mock(CoreCCMetadata.class);
        Mockito.when(bistatus.getSemComputationStatus()).thenReturn("FAILURE");
        Mockito.when(bistatus.getContinentalComputationStatus()).thenReturn("SUCCESS");

        Assertions.assertThat(MetadataUtil.generateOverallStatus(Set.of(bistatus), ComputationArea.ALL)).isEqualTo("PARTIAL_SUCCESS");
    }

    @Test
    void getFirstAndLastInstant() {
        final SoftAssertions assertions = new SoftAssertions();
        assertions.assertThat(MetadataUtil.getFirstInstant(null)).isEmpty();
        assertions.assertThat(MetadataUtil.getLastInstant(null)).isEmpty();
        assertions.assertThat(MetadataUtil.getFirstInstant(Set.of())).isEmpty();
        assertions.assertThat(MetadataUtil.getLastInstant(Set.of())).isEmpty();
        final Set<String> instantSet = Set.of("2026-10-15T14:00:00Z", "2026-10-15T15:00:00Z", "2026-10-19T09:00:00Z");
        assertions.assertThat(MetadataUtil.getFirstInstant(instantSet)).isEqualTo("2026-10-15T14:00:00Z");
        assertions.assertThat(MetadataUtil.getLastInstant(instantSet)).isEqualTo("2026-10-19T09:00:00Z");
        assertions.assertAll();
    }

    @Test
    void getResultsProvidedStatus() {
        final SoftAssertions assertions = new SoftAssertions();
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PARTIAL_SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("FAILURE")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PENDING")).isEqualTo("NO");
        assertions.assertAll();
    }

    @Test
    void getResultsProvidedStatus2() {
        final SoftAssertions assertions = new SoftAssertions();
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("SUCCESS", "SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("SUCCESS", "FAILURE")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("SUCCESS", "PENDING")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("SUCCESS", null)).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("FAILURE", "SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("FAILURE", "FAILURE")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("FAILURE", "PENDING")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("FAILURE", null)).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PENDING", "SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PENDING", "FAILURE")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PENDING", "PENDING")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus("PENDING", null)).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus(null, "SUCCESS")).isEqualTo("YES");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus(null, "FAILURE")).isEqualTo("NO");
        assertions.assertThat(MetadataUtil.getResultsProvidedStatus(null, "PENDING")).isEqualTo("NO");
        assertions.assertAll();
    }
}
