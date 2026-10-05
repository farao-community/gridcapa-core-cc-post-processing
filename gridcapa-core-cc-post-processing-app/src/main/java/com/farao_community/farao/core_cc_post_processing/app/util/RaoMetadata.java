/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public class RaoMetadata {
    private String timeInterval;
    private String raoRequestFileName;
    private String requestReceivedInstant;
    private String status;
    private String outputsSendingInstant;
    private String computationStartInstant;
    private String computationEndInstant;

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
