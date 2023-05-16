/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.CriticalBranchType;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.IndependantComplexVariant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
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
