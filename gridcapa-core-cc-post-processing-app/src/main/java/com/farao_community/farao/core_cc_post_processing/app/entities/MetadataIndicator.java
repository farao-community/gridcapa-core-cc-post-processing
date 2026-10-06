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
public enum MetadataIndicator {
    RAO_REQUESTS_RECEIVED("RAO requests received", 1), // per BD
    RAO_REQUEST_RECEPTION_TIME("RAO request reception time", 2), // per BD
    RAO_OUTPUTS_SENT("RAO outputs sent", 3), // per BD
    RAO_OUTPUTS_SENDING_TIME("RAO outputs sending time", 4), // per BD
    RAO_RESULTS_PROVIDED("RAO results provided", 5), // per TS
    RAO_COMPUTATION_STATUS("RAO computation status", 6), // per BD + per TS
    SEM_RAO_COMPUTATION_STATUS("SEM RAO computation status", 7), // per BD + per TS
    SEM_RAO_START_TIME("SEM RAO computation start", 8), // per BD + per TS
    SEM_RAO_END_TIME("SEM RAO computation end", 9), // per BD + per TS
    SEM_RAO_COMPUTATION_TIME("SEM RAO computation time (minutes)", 10), // per BD + per TS
    CONTINENTAL_RAO_COMPUTATION_STATUS("Continental RAO computation status", 11), // per BD + per TS
    CONTINENTAL_RAO_START_TIME("Continental RAO computation start", 12), // per BD + per TS
    CONTINENTAL_RAO_END_TIME("Continental RAO computation end", 13), // per BD + per TS
    CONTINENTAL_RAO_COMPUTATION_TIME("Continental RAO computation time (minutes)", 14); // per BD + per TS

    private final String csvLabel;
    private final int order;

    MetadataIndicator(String csvLabel, int order) {
        this.csvLabel = csvLabel;
        this.order = order;
    }

    public String getCsvLabel() {
        return this.csvLabel;
    }

    public int getOrder() {
        return order;
    }
}
