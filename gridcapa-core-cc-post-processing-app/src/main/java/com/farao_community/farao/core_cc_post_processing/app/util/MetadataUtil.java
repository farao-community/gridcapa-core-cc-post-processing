/*
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.entities.ComputationArea;
import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.resource.CoreCCMetadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataTaskStatus.FAILURE;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataTaskStatus.PARTIAL_SUCCESS;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataTaskStatus.PENDING;
import static com.farao_community.farao.core_cc_post_processing.app.entities.MetadataTaskStatus.SUCCESS;

/**
 * @author Vincent Bochet {@literal <vincent.bochet at rte-france.com>}
 */
public final class MetadataUtil {
    private static final String EMPTY_STRING = "";

    private MetadataUtil() {
    }

    static String computeStatusForTask(final String... statuses) {
        // Possible status values are PENDING, FAILURE and SUCCESS
        final Set<String> statusSet = Arrays.stream(statuses)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        if (statusSet.isEmpty()) {
            return null;
        }
        if (statusSet.size() == 1) {
            return statusSet.iterator().next();
        }
        if (statusSet.contains(SUCCESS)) {
            return PARTIAL_SUCCESS;
        }
        return FAILURE;
    }

    public static String generateOverallStatus(final Collection<CoreCCMetadata> metadataCollection, final ComputationArea computationArea) {
        final Set<String> statusSet = metadataCollection.stream()
            .map(metadata ->
                switch (computationArea) {
                    case CONTINENTAL -> computeStatusForTask(metadata.getContinentalComputationStatus());
                    case SEM -> computeStatusForTask(metadata.getSemComputationStatus());
                    default ->
                        computeStatusForTask(metadata.getContinentalComputationStatus(), metadata.getSemComputationStatus());
                }
            )
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        // Case where area was not computed: no status in input, no status in output
        if (computationArea != ComputationArea.ALL && statusSet.isEmpty()) {
            return EMPTY_STRING;
        }

        if (statusSet.stream().anyMatch(FAILURE::equals)) {
            return FAILURE;
        } else if (statusSet.stream().anyMatch(PENDING::equals)) {
            return PENDING;
        } else if (statusSet.stream().anyMatch(PARTIAL_SUCCESS::equals)) {
            return PARTIAL_SUCCESS;
        } else if (statusSet.stream().allMatch(SUCCESS::equals)) {
            return SUCCESS;
        } else {
            throw new CoreCCPostProcessingInternalException("Invalid overall status");
        }
    }

    public static String getFirstInstant(Set<String> instantSet) {
        if (instantSet == null || instantSet.isEmpty()) {
            return EMPTY_STRING;
        }
        return Collections.min(instantSet);
    }

    public static String getLastInstant(Set<String> instantSet) {
        if (instantSet == null || instantSet.isEmpty()) {
            return EMPTY_STRING;
        }
        return Collections.max(instantSet);
    }

    public static String getResultsProvidedStatus(final String taskStatus) {
        return SUCCESS.equals(taskStatus) || PARTIAL_SUCCESS.equals(taskStatus)
            ? "YES"
            : "NO";
    }

    public static String getResultsProvidedStatus(final String continentalStatus, final String semStatus) {
        final String overallStatus = generateOverallStatus(
            Set.of(
                new CoreCCMetadata.Builder()
                    .withContinentalComputationStatus(continentalStatus)
                    .withSemComputationStatus(semStatus)
                    .build()
            ),
            ComputationArea.ALL
        );
        return getResultsProvidedStatus(overallStatus);
    }
}
