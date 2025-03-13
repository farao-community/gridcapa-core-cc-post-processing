/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.threeten.extra.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public class DailyF303Generator {

    private DailyF303Generator() {
        throw new AssertionError("Static class. Should not be constructed");}

    public static FlowBasedConstraintDocument generate(DailyF303GeneratorInputsProvider inputsProvider) {
        FlowBasedConstraintDocument flowBasedConstraintDocument = inputsProvider.referenceConstraintDocument();
        Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(flowBasedConstraintDocument.getConstraintTimeInterval().getV());
        List<HourlyF303Info> hourlyF303Infos = new ArrayList<>();
        positionMap.values().forEach(interval -> {
            if (inputsProvider.shouldBeReported(interval)) {
                Optional<HourlyF303InfoGenerator.Inputs> optionalInputs = inputsProvider.hourlyF303InputsForInterval(interval);
                if (optionalInputs.isPresent()) {
                    hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForSuccessfulInterval(flowBasedConstraintDocument, interval, optionalInputs.get()));
                } else {
                    hourlyF303Infos.add(HourlyF303InfoGenerator.getInfoForNonRequestedOrFailedInterval(flowBasedConstraintDocument, interval));
                }
            }
        });

        // gather hourly info in one common document, cluster the elements that can be clusterized
        return new DailyF303Clusterizer(hourlyF303Infos, flowBasedConstraintDocument).generateClusterizedDocument();
    }
}
