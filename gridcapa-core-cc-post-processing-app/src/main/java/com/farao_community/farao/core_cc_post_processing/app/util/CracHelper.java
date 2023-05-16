/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.data.crac_creation.creator.api.parameters.CracCreationParameters;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.FbConstraint;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.crac_creator.FbConstraintCracCreator;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.crac_creator.FbConstraintCreationContext;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.importer.FbConstraintImporter;
import com.powsybl.iidm.network.Network;

import java.io.InputStream;
import java.time.OffsetDateTime;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class CracHelper {

    private CracHelper() {
        throw new AssertionError("Utility class should not be constructed");
    }

    public static CracCreationParameters getCracCreationParameters() {
        CracCreationParameters parameters = CracCreationParameters.load();
        parameters.setDefaultMonitoredLineSide(CracCreationParameters.MonitoredLineSide.MONITOR_LINES_ON_LEFT_SIDE);
        return parameters;
    }

    public static FbConstraintCreationContext importCracXmlGetFbInfoWithNetwork(String timestamp, Network network, InputStream cracXmlFileInputStream) {
        try {
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestamp);
            FbConstraint fbConstraint = new FbConstraintImporter().importNativeCrac(cracXmlFileInputStream);
            FbConstraintCreationContext cracCreationContext = new FbConstraintCracCreator().createCrac(fbConstraint, network, offsetDateTime, getCracCreationParameters());
            if (cracCreationContext.isCreationSuccessful()) {
                return cracCreationContext;
            } else {
                throw new CoreCCPostProcessingInternalException("Crac creation context failed for timestamp: " + timestamp);
            }
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Crac creation context failed, timestamp: " + timestamp, e);
        }
    }
}
