package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;

import java.util.Set;
import java.util.TreeSet;

public class RaoMetadata {
    String raoRequestFileName;
    String timeInterval;
    String requestReceivedInstant;
    String outputsSendingInstant;
    String status;
    int version;
    String computationStartInstant;
    String computationEndInstant;
    String instant;

    public enum Indicator {
        RAO_REQUESTS_RECEIVED("RAO requests received", 1), // per BD
        RAO_REQUEST_RECEPTION_TIME("RAO request reception time", 2), // per BD
        RAO_OUTPUTS_SENT("RAO outputs sent", 3), // per BD
        RAO_OUTPUTS_SENDING_TIME("RAO outputs sending time", 4), // per BD
        RAO_RESULTS_PROVIDED("RAO results provided", 5), // per TS
        RAO_COMPUTATION_STATUS("RAO computation status", 6), // per BD + per TS
        RAO_START_TIME("RAO computation start", 7), // per BD + per TS
        RAO_END_TIME("RAO computation end", 8), // per BD + per TS
        RAO_COMPUTATION_TIME("RAO computation time (minutes)", 9); // per BD + per TS

        private String csvLabel;
        private int order;
        Indicator(String csvLabel, int order) {
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

    public static String generateOverallStatus(Set<String> statusSet) {
        if (statusSet.stream().anyMatch(s -> s.equals("FAILURE"))) {
            return "FAILURE";
        } else if (statusSet.stream().anyMatch(s -> s.equals("PENDING"))) {
            return "PENDING";
        } else if (statusSet.stream().anyMatch(s -> s.equals("RUNNING"))) {
            return "RUNNING";
        } else if (statusSet.stream().allMatch(s -> s.equals("SUCCESS"))) {
            return "SUCCESS";
        } else {
            throw new CoreCCPostProcessingInternalException("Invalid overall status");
        }
    }

    public static String getFirstInstant(Set<String> instantSet) {
        TreeSet ts = new TreeSet();
        ts.addAll(instantSet);
        return ts.first().toString();
    }

    public static String getLastInstant(Set<String> instantSet) {
        TreeSet ts = new TreeSet();
        ts.addAll(instantSet);
        return ts.last().toString();
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getInstant() {
        return instant;
    }

    public void setInstant(String instant) {
        this.instant = instant;
    }
}
