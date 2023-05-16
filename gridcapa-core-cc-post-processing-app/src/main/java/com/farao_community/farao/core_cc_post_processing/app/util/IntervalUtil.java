/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.threeten.extra.Interval;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class IntervalUtil {

    private IntervalUtil() {
        throw new AssertionError("Utility class should not be constructed");
    }

    // key is the position (start position is 1, end position is 24, but could be  23 or 25 in daylight saving)
    public static Map<Integer, Interval> getPositionsMap(String intervalString) {

        Interval interval = Interval.parse(intervalString);
        Instant startDate = interval.getStart();
        Instant endDate = interval.getEnd();
        Map<Integer, Interval> positionsMap = new TreeMap<>();
        int position = 1;
        while (startDate.isBefore(endDate)) {
            positionsMap.put(position, Interval.of(startDate, startDate.plusSeconds(3600)));
            startDate = startDate.plusSeconds(3600);
            position++;
        }
        return positionsMap;
    }

    public static int getInstantPosition(String instant, String interval) {
        Map<Integer, Interval> positionsMap = getPositionsMap(interval);
        return positionsMap.entrySet().stream()
            .filter(entry -> entry.getValue().contains(Instant.parse(instant)))
            .findFirst()
            .orElseThrow(() -> new CoreCCPostProcessingInternalException(String.format("Instant %s not found in interval %s", instant, interval)))
            .getKey();
    }

    public static String getTsMiddleOfHourlyInterval(Interval interval) {
        return interval.getStart().plus(30, ChronoUnit.MINUTES).toString();
    }

    public static String getCurrentTimeInterval(OffsetDateTime timestamp) {
        OffsetDateTime startTime = timestamp.minusMinutes(timestamp.getMinute());
        OffsetDateTime endTime = startTime.plusHours(1L);
        String start = startTime.toString();
        return start + "/" + endTime;
    }

    public static boolean isInTimeInterval(OffsetDateTime offsetDateTime, String timeInterval) {
        String[] timeIntervals = timeInterval.split("/");
        OffsetDateTime startTimeBranch = OffsetDateTime.parse(timeIntervals[0]);
        OffsetDateTime endTimeBranch = OffsetDateTime.parse(timeIntervals[1]);
        return !offsetDateTime.isBefore(startTimeBranch) && offsetDateTime.isBefore(endTimeBranch);
    }

    public static OffsetDateTime getBusinessDay(String timeInterval) {
        return OffsetDateTime.parse(timeInterval.split("/")[1]).withHour(0);
    }

    public static String getFormattedBusinessDay(String timeInterval, String format) {
        return DateTimeFormatter.ofPattern(format).format(getBusinessDay(timeInterval));
    }

    public static String getFormattedBusinessDay(LocalDate localDate, String format) {
        return DateTimeFormatter.ofPattern(format).format(localDate);
    }

    public static String getFormattedBusinessDay(String timeInterval) {
        return getFormattedBusinessDay(timeInterval, "yyyyMMdd");
    }

    public static String getFormattedBusinessDay(LocalDate localDate) {
        return getFormattedBusinessDay(localDate, "yyyyMMdd");
    }

    public static Interval getInterval(LocalDate localDate) {
        Instant startOfDay = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant endOfDay = startOfDay.plus(1, ChronoUnit.DAYS);
        return Interval.of(startOfDay, endOfDay);
    }
}
