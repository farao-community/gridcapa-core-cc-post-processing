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
import java.util.Objects;

import static com.farao_community.farao.core_cc_post_processing.app.util.CracUtil.importNativeCrac;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class HourlyFbConstraintInfoGeneratorTest {

    private FlowBasedConstraintDocument nativeCrac;
    private final Instant instantStart = Instant.parse("2023-08-21T15:16:00Z");
    private final Instant instantEnd = Instant.parse("2023-08-21T15:17:00Z");
    private final Interval interval = Interval.of(instantStart, instantEnd);
    private TaskDto taskDto;
    private final MinioAdapter minioAdapter = mock(MinioAdapter.class);
    private InputStream networkIS;
    private InputStream raoResultIS;
    private InputStream cracInputStream;

    @BeforeEach
    void setUp() throws IOException {
        importCrac();
    }

    private void importCrac() throws IOException {
        Path cracPath = Paths.get(Objects.requireNonNull(getClass().getResource("/services/crac.xml")).getPath());
        File cracFile = new File(cracPath.toString());
        cracInputStream = new FileInputStream(cracFile);
        nativeCrac = importNativeCrac(new FileInputStream(cracFile));
    }

    private void importNetwork() throws FileNotFoundException {
        Path networkPath = Paths.get(Objects.requireNonNull(getClass().getResource("/services/network.uct")).getPath());
        File networkFile = new File(networkPath.toString());
        networkIS = new FileInputStream(networkFile);
    }

    private void importRaoResult() throws FileNotFoundException {
        Path raoResultPath = Paths.get(Objects.requireNonNull(getClass().getResource("/services/raoResult.json")).getPath());
        File raoResultFile = new File(raoResultPath.toString());
        raoResultIS = new FileInputStream(raoResultFile);
    }

    @Test
    void generate() throws FileNotFoundException {
        importNetwork();
        importRaoResult();
        taskDto = Utils.SUCCESS_TASK_2023;
        doReturn(networkIS).when(minioAdapter).getFileFromFullPath("network.uct");
        doReturn(raoResultIS).when(minioAdapter).getFileFromFullPath("raoResult.json");
        //crac creation parameters
        final CracCreationParameters cracCreationParameters = JsonCracCreationParameters.read(getClass().getResourceAsStream("/crac/cracCreationParameters.json"));
        HourlyFbConstraintInfoGenerator hourlyFbConstraintInfoGenerator = new HourlyFbConstraintInfoGenerator(nativeCrac, interval, taskDto, minioAdapter, cracCreationParameters);
        final ProcessFileDto processFileDto = new ProcessFileDto("raoResult.json", "", ProcessFileStatus.VALIDATED, "raoResult.json", "docId", OffsetDateTime.now());
        final ProcessFileDto cgmProcessFile = new ProcessFileDto("network.uct", "", ProcessFileStatus.VALIDATED, "network.uct", "docId", OffsetDateTime.now());
        //
        HourlyFbConstraintInfo hourlyFbConstraintInfo = hourlyFbConstraintInfoGenerator.generate(processFileDto, cgmProcessFile, cracInputStream);
        checkCriticalBranchesWithTatlPatl(hourlyFbConstraintInfo);
        checkComplexVariants(hourlyFbConstraintInfo);
    }

    private static void checkComplexVariants(HourlyFbConstraintInfo hourlyFbConstraintInfo) {
        assertEquals(1, hourlyFbConstraintInfo.getComplexVariants().size());
        assertEquals("CRA_150001", hourlyFbConstraintInfo.getComplexVariants().get(0).getId());
        assertEquals(2, hourlyFbConstraintInfo.getComplexVariants().get(0).getActionsSet().size());
        assertEquals("open_fr1_fr3", hourlyFbConstraintInfo.getComplexVariants().get(0).getActionsSet().get(0).getName());
        assertEquals("pst_be", hourlyFbConstraintInfo.getComplexVariants().get(0).getActionsSet().get(1).getName());
    }

    private static void checkCriticalBranchesWithTatlPatl(HourlyFbConstraintInfo hourlyFbConstraintInfo) {
        assertEquals(11, hourlyFbConstraintInfo.getCriticalBranches().size());
        assertEquals("de2_nl3_N", hourlyFbConstraintInfo.getCriticalBranches().get(0).getId());
        assertEquals("fr4_de1_N", hourlyFbConstraintInfo.getCriticalBranches().get(1).getId());
        assertEquals("nl2_be3_N", hourlyFbConstraintInfo.getCriticalBranches().get(2).getId());
        assertEquals("fr3_fr5_CO1 - DIR_TATL", hourlyFbConstraintInfo.getCriticalBranches().get(3).getId());
        assertEquals("fr3_fr5_CO1 - DIR_PATL", hourlyFbConstraintInfo.getCriticalBranches().get(4).getId());
        assertEquals("fr1_fr4_CO1_TATL", hourlyFbConstraintInfo.getCriticalBranches().get(5).getId());
        assertEquals("fr1_fr4_CO1_PATL", hourlyFbConstraintInfo.getCriticalBranches().get(6).getId());
        assertEquals("fr4_de1_CO1_TATL", hourlyFbConstraintInfo.getCriticalBranches().get(7).getId());
        assertEquals("fr4_de1_CO1_PATL", hourlyFbConstraintInfo.getCriticalBranches().get(8).getId());
        assertEquals("fr3_fr5_CO1 - OPP_TATL", hourlyFbConstraintInfo.getCriticalBranches().get(9).getId());
        assertEquals("fr3_fr5_CO1 - OPP_PATL", hourlyFbConstraintInfo.getCriticalBranches().get(10).getId());
    }

    @Test
    void generateForNullTask() {
        HourlyFbConstraintInfoGenerator hourlyFbConstraintInfoGenerator = new HourlyFbConstraintInfoGenerator(nativeCrac, interval, null, minioAdapter, new CracCreationParameters());
        HourlyFbConstraintInfo hourlyFbConstraintInfo = hourlyFbConstraintInfoGenerator.generate(null, null, null);
        checkCriticalBranches(hourlyFbConstraintInfo);
    }

    @Test
    void generateForNotSuccessfulTask() {
        taskDto = Utils.ERROR_TASK_2023;
        HourlyFbConstraintInfoGenerator hourlyFbConstraintInfoGenerator = new HourlyFbConstraintInfoGenerator(nativeCrac, interval, taskDto, minioAdapter, new CracCreationParameters());
        HourlyFbConstraintInfo hourlyFbConstraintInfo = hourlyFbConstraintInfoGenerator.generate(null, null, cracInputStream);
        checkCriticalBranches(hourlyFbConstraintInfo);
    }

    private static void checkCriticalBranches(HourlyFbConstraintInfo hourlyFbConstraintInfo) {
        assertEquals(7, hourlyFbConstraintInfo.getCriticalBranches().size());
        assertEquals("fr4_de1_N", hourlyFbConstraintInfo.getCriticalBranches().get(0).getId());
        assertEquals("nl2_be3_N", hourlyFbConstraintInfo.getCriticalBranches().get(1).getId());
        assertEquals("de2_nl3_N", hourlyFbConstraintInfo.getCriticalBranches().get(2).getId());
        assertEquals("fr4_de1_CO1", hourlyFbConstraintInfo.getCriticalBranches().get(3).getId());
        assertEquals("fr3_fr5_CO1 - DIR", hourlyFbConstraintInfo.getCriticalBranches().get(4).getId());
        assertEquals("fr3_fr5_CO1 - OPP", hourlyFbConstraintInfo.getCriticalBranches().get(5).getId());
        assertEquals("fr1_fr4_CO1", hourlyFbConstraintInfo.getCriticalBranches().get(6).getId());
    }
}
