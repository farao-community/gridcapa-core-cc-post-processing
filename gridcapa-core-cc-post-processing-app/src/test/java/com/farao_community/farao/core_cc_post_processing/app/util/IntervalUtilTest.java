/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class IntervalUtilTest {

    @Test
    void getPositionsMap() {
        String intervalString = "2023-08-21T23:00:00Z/2023-08-22T23:00:00Z";
        Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(intervalString);
        assertEquals(24, positionMap.size());
        assertEquals(positionMap.get(1), Interval.of(Instant.parse("2023-08-21T23:00:00Z"), Instant.parse("2023-08-22T00:00:00Z")));
        assertEquals(positionMap.get(24), Interval.of(Instant.parse("2023-08-22T22:00:00Z"), Instant.parse("2023-08-22T23:00:00Z")));
    }
}
