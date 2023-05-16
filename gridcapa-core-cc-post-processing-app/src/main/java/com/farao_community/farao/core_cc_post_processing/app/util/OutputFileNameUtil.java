/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class OutputFileNameUtil {

    private OutputFileNameUtil() {
    }

    public static String generateRaoRequestAckFileName(LocalDate localDate) {
        return OutputsNamingRules.RAO_REQUEST_ACK_FILENAME_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateRaoIResponseFileName(LocalDate localDate) {
        return OutputsNamingRules.RAO_INTEGRATION_RESPONSE_FILENAME_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateUctFileName(String instant, int version) {
        String output = OutputsNamingRules.UCT_FILENAME_FORMATTER.format(Instant.parse(instant));
        output = output.replace("2D0", "2D" + Instant.parse(instant).atZone(OutputsNamingRules.ZONE_ID).getDayOfWeek().getValue())
                .replace("_UXV", "_UX" + version);
        return handle25TimestampCase(output, instant);
    }

    public static String generateCneFileName(String instant) {
        String output = OutputsNamingRules.CNE_FILENAME_FORMATTER.format(Instant.parse(instant))
                .replace("-v0-", "-v" + 1 + "-");
        return handle25TimestampCase(output, instant);
    }

    public static String generateOptimizedCbFileName(LocalDate localDate) {
        return OutputsNamingRules.OPTIMIZED_CB_FILENAME_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateOutputsDestinationPath(String destinationPrefix, String fileName) {
        return String.format(OutputsNamingRules.OUTPUTS, destinationPrefix, fileName);
    }

    public static String generateCgmZipName(LocalDate localDate) {
        return OutputsNamingRules.UCT_OUTPUT_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateCneZipName(LocalDate localDate) {
        return OutputsNamingRules.CNE_OUTPUT_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateMetadataFileName(LocalDate localDate) {
        return OutputsNamingRules.METADATA_FILENAME_FORMATTER.format(localDate)
                .replace("0V", String.format("%02d", 1));
    }

    public static String generateCracCreationReportFileName(String instant, LocalDate localDate) {
        String output = OutputsNamingRules.RAO_LOGS_FILENAME_FORMATTER.format(Instant.parse(instant))
            .replace("0V", String.format("%02d", 1));
        return handle25TimestampCase(output, instant);
    }

    public static String generateLogsZipName(LocalDate localDate) {
        return OutputsNamingRules.LOGS_OUTPUT_FORMATTER.format(localDate)
            .replace("0V", String.format("%02d", 1));
    }

    /**
     * For 25-timestamp business day, replace the duplicate hour "_HH30_" with "_BH30_"
     */
    private static String handle25TimestampCase(String filename, String instant) {
        ZoneOffset previousOffset = OffsetDateTime.from(Instant.parse(instant).minus(1, ChronoUnit.HOURS).atZone(OutputsNamingRules.ZONE_ID)).getOffset();
        ZoneOffset currentOffset = OffsetDateTime.from(Instant.parse(instant).atZone(OutputsNamingRules.ZONE_ID)).getOffset();
        if (previousOffset == ZoneOffset.ofHours(2) && currentOffset == ZoneOffset.ofHours(1)) {
            return filename.replace("_0", "_B");
        } else {
            return filename;
        }
    }
}
