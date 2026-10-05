/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public class RaoMetadata {

    private static final String EMPTY_STRING = "";
    String timeInterval;
    String raoRequestFileName;
    String requestReceivedInstant;
    String status;
    String outputsSendingInstant;
    String computationStartInstant;
    String computationEndInstant;

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
        TreeSet<String> ts = new TreeSet<>();
        ts.addAll(instantSet);
        return ts.first();
    }

    public static String getLastInstant(Set<String> instantSet) {
        if (instantSet == null || instantSet.isEmpty()) {
            return EMPTY_STRING;
        }
        TreeSet<String> ts = new TreeSet<>();
        ts.addAll(instantSet);
        return ts.last();
    }

    public String getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(String timeInterval) {
        this.timeInterval = timeInterval;
    }

    public String getRaoRequestFileName() {
        return raoRequestFileName;
    }

    public void setRaoRequestFileName(String raoRequestFileName) {
        this.raoRequestFileName = raoRequestFileName;
    }

    public String getRequestReceivedInstant() {
        return requestReceivedInstant;
    }

    public void setRequestReceivedInstant(String requestReceivedInstant) {
        this.requestReceivedInstant = requestReceivedInstant;
    }

    public String getOutputsSendingInstant() {
        return outputsSendingInstant;
    }

    public void setOutputsSendingInstant(String outputsSendingInstant) {
        this.outputsSendingInstant = outputsSendingInstant;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getComputationStartInstant() {
        return computationStartInstant;
    }

    public void setComputationStartInstant(String computationStartInstant) {
        this.computationStartInstant = computationStartInstant;
    }

    public String getComputationEndInstant() {
        return computationEndInstant;
    }

    public void setComputationEndInstant(String computationEndInstant) {
        this.computationEndInstant = computationEndInstant;
    }
}
