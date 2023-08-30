/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.CriticalBranchType;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.IndependantComplexVariant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
class HourlyF303Info {

    private List<CriticalBranchType> criticalBranches;
    private List<IndependantComplexVariant> complexVariants;

    HourlyF303Info(List<CriticalBranchType> criticalBranches) {
        this.criticalBranches = criticalBranches;
        this.complexVariants = new ArrayList<>();
    }

    HourlyF303Info(List<CriticalBranchType> criticalBranches, List<IndependantComplexVariant> complexVariants) {
        this.criticalBranches = criticalBranches;
        this.complexVariants = complexVariants;
    }

    List<CriticalBranchType> getCriticalBranches() {
        return criticalBranches;
    }

    List<IndependantComplexVariant> getComplexVariants() {
        return complexVariants;
    }
}
