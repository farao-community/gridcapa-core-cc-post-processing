/*
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;

import java.util.Collections;
import java.util.Set;

/**
 * @author Vincent Bochet {@literal <vincent.bochet at rte-france.com>}
 */
public final class MetadataUtil {
    private static final String EMPTY_STRING = "";

    private MetadataUtil() {
    }

    public static String generateOverallStatus(Set<String> statusSet) {
        if (statusSet.stream().anyMatch(s -> s.equals("FAILURE"))) {
            return "FAILURE";
        } else if (statusSet.stream().anyMatch(s -> s.equals("PENDING"))) {
            return "PENDING";
        } else if (statusSet.stream().anyMatch(s -> s.equals("RUNNING"))) {
            throw new CoreCCInternalException("No task should be set to RUNNING");
        } else if (statusSet.stream().allMatch(s -> s.equals("SUCCESS"))) {
            return "SUCCESS";
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
}
