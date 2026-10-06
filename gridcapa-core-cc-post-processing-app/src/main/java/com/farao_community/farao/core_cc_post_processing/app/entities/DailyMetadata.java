/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.entities;

/**
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public class DailyMetadata {
    private String timeInterval;
    private String raoRequestFileName;
    private String requestReceivedInstant;
    private String status;
    private String outputsSendingInstant;
    private String continentalComputationStatus;
    private String continentalComputationStart;
    private String continentalComputationEnd;
    private String semComputationStatus;
    private String semComputationStart;
    private String semComputationEnd;

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

    public String getContinentalComputationStatus() {
        return continentalComputationStatus;
    }

    public void setContinentalComputationStatus(final String continentalComputationStatus) {
        this.continentalComputationStatus = continentalComputationStatus;
    }

    public String getContinentalComputationStart() {
        return continentalComputationStart;
    }

    public void setContinentalComputationStart(final String continentalComputationStart) {
        this.continentalComputationStart = continentalComputationStart;
    }

    public String getContinentalComputationEnd() {
        return continentalComputationEnd;
    }

    public void setContinentalComputationEnd(final String continentalComputationEnd) {
        this.continentalComputationEnd = continentalComputationEnd;
    }

    public String getSemComputationStatus() {
        return semComputationStatus;
    }

    public void setSemComputationStatus(final String semComputationStatus) {
        this.semComputationStatus = semComputationStatus;
    }

    public String getSemComputationStart() {
        return semComputationStart;
    }

    public void setSemComputationStart(final String semComputationStart) {
        this.semComputationStart = semComputationStart;
    }

    public String getSemComputationEnd() {
        return semComputationEnd;
    }

    public void setSemComputationEnd(final String semComputationEnd) {
        this.semComputationEnd = semComputationEnd;
    }
}
