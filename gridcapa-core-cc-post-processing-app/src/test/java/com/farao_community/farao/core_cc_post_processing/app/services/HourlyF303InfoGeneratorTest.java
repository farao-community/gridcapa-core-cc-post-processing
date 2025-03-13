/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.threeten.extra.Interval;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;

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
    private TaskDto taskDto;
    private final MinioAdapter minioAdapter = Mockito.mock(MinioAdapter.class);

    @BeforeEach
    void setUp() {
        importCrac();
    }

    private void importCrac() {
        nativeCrac = importNativeCrac(getClass().getResourceAsStream("/services/crac.xml"));
    }

    @Test
    void generate() {
        Mockito.doAnswer((Answer<InputStream>) invocation -> getClass().getResourceAsStream("/services/network.uct")).when(minioAdapter).getFileFromFullPath("network.uct");
        Mockito.doAnswer((Answer<InputStream>) invocation -> getClass().getResourceAsStream("/services/raoResult.json")).when(minioAdapter).getFileFromFullPath("raoResult.json");
        Mockito.doAnswer((Answer<InputStream>) invocation -> getClass().getResourceAsStream("/services/crac.xml")).when(minioAdapter).getFileFromFullPath("crac.xml");
        //crac creation parameters
        final CracCreationParameters cracCreationParameters = JsonCracCreationParameters.read(getClass().getResourceAsStream("/services/crac/cracCreationParameters.json"));
        final ProcessFileDto cracFileDto = new ProcessFileDto("crac.xml", "CBCORA", ProcessFileStatus.VALIDATED, "crac.xml", "docId", OffsetDateTime.now());
        final ProcessFileDto raoResultFileDto = new ProcessFileDto("raoResult.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult.json", "docId", OffsetDateTime.now());
        final ProcessFileDto cgmProcessFile = new ProcessFileDto("network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", "docId", OffsetDateTime.now());
        taskDto = Utils.successTaskWithInputsAndOutputs(List.of(cracFileDto), List.of(raoResultFileDto, cgmProcessFile));
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter, cracCreationParameters);
        //
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate();
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
    void generateForNullTask() {
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, null, minioAdapter, new CracCreationParameters());
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate();
        checkCriticalBranches(hourlyF303Info);
    }

    @Test
    void generateForNotSuccessfulTask() {
        taskDto = Utils.ERROR_TASK;
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter, new CracCreationParameters());
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate();
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
