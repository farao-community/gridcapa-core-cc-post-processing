/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.entities;

import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.CriticalBranchType;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.IndependantComplexVariant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public class HourlyFbConstraintInfo {

    private final List<CriticalBranchType> criticalBranches;
    private final List<IndependantComplexVariant> complexVariants;

    public HourlyFbConstraintInfo(List<CriticalBranchType> criticalBranches) {
        this.criticalBranches = criticalBranches;
        this.complexVariants = new ArrayList<>();
    }

    public HourlyFbConstraintInfo(List<CriticalBranchType> criticalBranches, List<IndependantComplexVariant> complexVariants) {
        this.criticalBranches = criticalBranches;
        this.complexVariants = complexVariants;
    }

    public List<CriticalBranchType> getCriticalBranches() {
        return criticalBranches;
    }

    public List<IndependantComplexVariant> getComplexVariants() {
        return complexVariants;
    }
}
