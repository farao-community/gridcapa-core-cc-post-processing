/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintCreationContext;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintImporter;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;

import java.io.*;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;

import static com.farao_community.farao.core_cc_post_processing.app.util.CracUtil.importNativeCrac;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class HourlyF303InfoGeneratorTest {

    private FlowBasedConstraintDocument nativeCrac;
    private final Instant instantStart = Instant.parse("2023-08-21T15:16:00Z");
    private final Instant instantEnd = Instant.parse("2023-08-21T15:17:00Z");
    private final Interval interval = Interval.of(instantStart, instantEnd);

    @BeforeEach
    void setUp() {
        importCrac();
    }

    private void importCrac() {
        nativeCrac = importNativeCrac(getClass().getResourceAsStream("/services/crac.xml"));
    }

    private Network networkFromResources(String resourcePath) {
        return Network.read(Path.of(resourcePath).getFileName().toString(), getClass().getResourceAsStream(resourcePath));
    }

    private FbConstraintCreationContext cracFromResources(Network network, OffsetDateTime timestamp, String resourcePath) {
        final CracCreationParameters cracCreationParameters = JsonCracCreationParameters.read(getClass().getResourceAsStream("/services/crac/cracCreationParameters.json"));
        return (FbConstraintCreationContext) new FbConstraintImporter().importData(getClass().getResourceAsStream(resourcePath), cracCreationParameters, network, timestamp);
    }

    private RaoResult taoResultFromResource(FbConstraintCreationContext crac, String resourcePath) {
        try {
            return RaoResult.read(getClass().getResourceAsStream(resourcePath), crac.getCrac());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void generate() {
        OffsetDateTime timestamp = OffsetDateTime.parse("2023-08-21T15:16:45Z");
        Network network = networkFromResources("/services/network.uct");
        FbConstraintCreationContext crac = cracFromResources(network, timestamp, "/services/crac.xml");
        RaoResult raoResult = taoResultFromResource(crac, "/services/raoResult.json");
        HourlyF303Info hourlyF303Info = HourlyF303InfoGenerator.getInfoForSuccessfulInterval(nativeCrac, interval, timestamp, crac, raoResult);
        checkCriticalBranchesWithTatlPatl(hourlyF303Info);
        checkComplexVariants(hourlyF303Info);
    }

    private static void checkComplexVariants(HourlyF303Info hourlyF303Info) {
        assertEquals(1, hourlyF303Info.getComplexVariants().size());
        assertEquals("CRA_150001", hourlyF303Info.getComplexVariants().get(0).getId());
        assertEquals(2, hourlyF303Info.getComplexVariants().get(0).getActionsSet().size());
        assertEquals("open_fr1_fr3", hourlyF303Info.getComplexVariants().get(0).getActionsSet().get(0).getName());
        assertEquals("pst_be", hourlyF303Info.getComplexVariants().get(0).getActionsSet().get(1).getName());
    }

    private static void checkCriticalBranchesWithTatlPatl(HourlyF303Info hourlyF303Info) {
        assertEquals(11, hourlyF303Info.getCriticalBranches().size());
        assertEquals("de2_nl3_N", hourlyF303Info.getCriticalBranches().get(0).getId());
        assertEquals("fr4_de1_N", hourlyF303Info.getCriticalBranches().get(1).getId());
        assertEquals("nl2_be3_N", hourlyF303Info.getCriticalBranches().get(2).getId());
        assertEquals("fr3_fr5_CO1 - DIR_TATL", hourlyF303Info.getCriticalBranches().get(3).getId());
        assertEquals("fr3_fr5_CO1 - DIR_PATL", hourlyF303Info.getCriticalBranches().get(4).getId());
        assertEquals("fr1_fr4_CO1_TATL", hourlyF303Info.getCriticalBranches().get(5).getId());
        assertEquals("fr1_fr4_CO1_PATL", hourlyF303Info.getCriticalBranches().get(6).getId());
        assertEquals("fr4_de1_CO1_TATL", hourlyF303Info.getCriticalBranches().get(7).getId());
        assertEquals("fr4_de1_CO1_PATL", hourlyF303Info.getCriticalBranches().get(8).getId());
        assertEquals("fr3_fr5_CO1 - OPP_TATL", hourlyF303Info.getCriticalBranches().get(9).getId());
        assertEquals("fr3_fr5_CO1 - OPP_PATL", hourlyF303Info.getCriticalBranches().get(10).getId());
    }

    @Test
    void generateForError() {
        HourlyF303Info hourlyF303Info = HourlyF303InfoGenerator.getInfoForNonRequestedOrFailedInterval(nativeCrac, interval);
        checkCriticalBranches(hourlyF303Info);
    }

    private static void checkCriticalBranches(HourlyF303Info hourlyF303Info) {
        assertEquals(7, hourlyF303Info.getCriticalBranches().size());
        assertEquals("fr4_de1_N", hourlyF303Info.getCriticalBranches().get(0).getId());
        assertEquals("nl2_be3_N", hourlyF303Info.getCriticalBranches().get(1).getId());
        assertEquals("de2_nl3_N", hourlyF303Info.getCriticalBranches().get(2).getId());
        assertEquals("fr4_de1_CO1", hourlyF303Info.getCriticalBranches().get(3).getId());
        assertEquals("fr3_fr5_CO1 - DIR", hourlyF303Info.getCriticalBranches().get(4).getId());
        assertEquals("fr3_fr5_CO1 - OPP", hourlyF303Info.getCriticalBranches().get(5).getId());
        assertEquals("fr1_fr4_CO1", hourlyF303Info.getCriticalBranches().get(6).getId());
    }

}
