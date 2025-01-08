/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import com.farao_community.farao.gridcapa_core_cc.api.util.IntervalUtil;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public final class NamingRules {
    private NamingRules() {
        throw new AssertionError("Utility class should not be constructed");
    }

    public static final String OUTPUTS = "%s/outputs/%s"; // destination/filename

    // DateTimeFormatter are systematically rezoned even applied on offsetDateTimes as a security measure
    public static final DateTimeFormatter UCT_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'_'HH'30_2D0_UXV.uct'").withZone(IntervalUtil.ZONE_ID);
    public static final DateTimeFormatter UCT_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-304_'yyyyMMdd'-F304-<version>.zip'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)
    public static final DateTimeFormatter F305_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-305_'yyyyMMdd'-F305-<version>.xml'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)
    public static final DateTimeFormatter CNE_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A43-299_'yyyyMMdd'-F299-<version>.zip'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)
    public static final DateTimeFormatter OPTIMIZED_CB_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-B06A01-303_'yyyyMMdd'-F303-<version>.xml'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)
    public static final String CGM_XML_HEADER_FILENAME = "CGM_XML_Header.xml";
    public static final DateTimeFormatter LOGS_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-342_'yyyyMMdd'-F342-<version>.zip'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)
    public static final DateTimeFormatter METADATA_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("'22XCORESO------S_10V1001C--00236Y_CORE-FB-341_'yyyyMMdd'-F341-<version>.csv'").withZone(IntervalUtil.ZONE_ID); // NOSONAR because a bug in the java:S3986 rule triggers a warning on the use of "Y" in the pattern even if it is escaped (placed between single quotes in order to be used as String and not to be interpreted by the formatter)

    private static final DateTimeFormatter RAO_RESULT_ZIP_FORMATTER = DateTimeFormatter.ofPattern("'CASTOR-INTERNAL-RESULTS_'yyyyMMdd'.zip'");

    public static String generateRF305FileName(LocalDate localDate, int version) {
        return formatVersion(F305_FILENAME_FORMATTER.format(localDate), version);
    }

    public static String generateUctFileName(String instant, int version) {
        String output = UCT_FILENAME_FORMATTER.format(Instant.parse(instant));
        output = output.replace("2D0", "2D" + Instant.parse(instant).atZone(IntervalUtil.ZONE_ID).getDayOfWeek().getValue())
            .replace("_UXV", "_UX" + version);
        return com.farao_community.farao.gridcapa_core_cc.api.util.IntervalUtil.handle25TimestampCase(output, instant);
    }

    public static String generateOptimizedCbFileName(LocalDate localDate, int version) {
        return formatVersion(OPTIMIZED_CB_FILENAME_FORMATTER.format(localDate), version);
    }

    public static String generateOutputsDestinationPath(String destinationPrefix, String fileName) {
        return String.format(OUTPUTS, destinationPrefix, fileName);
    }

    public static String generateCgmZipName(LocalDate localDate, int version) {
        return formatVersion(UCT_OUTPUT_FORMATTER.format(localDate), version);
    }

    public static String generateCneZipName(LocalDate localDate, int version) {
        return formatVersion(CNE_OUTPUT_FORMATTER.format(localDate), version);
    }

    public static String generateZippedLogsName(String instant, String outputsTargetMinioFolder, int version) {
        return String.format(OUTPUTS, outputsTargetMinioFolder, formatVersion(NamingRules.LOGS_OUTPUT_FORMATTER.format(Instant.parse(instant)), version));
    }

    public static String generateMetadataFileName(String instant, int version) {
        return formatVersion(METADATA_FILENAME_FORMATTER.format(Instant.parse(instant)), version);
    }

    public static String generateRaoResultFilename(LocalDate localDate) {
        return RAO_RESULT_ZIP_FORMATTER.format(localDate);
    }

    private static String formatVersion(String filename, int v) {
        return filename.replace("<version>", String.format("%02d", v));
    }
}
