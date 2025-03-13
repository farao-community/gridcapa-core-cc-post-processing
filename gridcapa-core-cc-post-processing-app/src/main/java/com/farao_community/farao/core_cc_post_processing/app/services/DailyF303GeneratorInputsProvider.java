package com.farao_community.farao.core_cc_post_processing.app.services;

import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.threeten.extra.Interval;

import java.util.Optional;

public interface DailyF303GeneratorInputsProvider {
    FlowBasedConstraintDocument referenceConstraintDocument();

    Optional<HourlyF303InfoGenerator.Inputs> hourlyF303InputsForInterval(Interval interval);
}
