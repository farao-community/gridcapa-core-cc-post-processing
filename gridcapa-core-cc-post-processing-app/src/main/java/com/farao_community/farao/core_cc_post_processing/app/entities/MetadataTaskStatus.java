/*
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.entities;

/**
 * @author Vincent Bochet {@literal <vincent.bochet at rte-france.com>}
 */
public final class MetadataTaskStatus {
    private MetadataTaskStatus() {
    }

    public static final String SUCCESS = "SUCCESS";
    public static final String FAILURE = "FAILURE";
    public static final String PENDING = "PENDING";
    public static final String PARTIAL_SUCCESS = "PARTIAL_SUCCESS";
}
