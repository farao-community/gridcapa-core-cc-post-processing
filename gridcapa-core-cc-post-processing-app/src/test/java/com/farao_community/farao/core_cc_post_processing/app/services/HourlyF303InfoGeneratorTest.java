/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.FbConstraint;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.importer.FbConstraintImporter;
import com.farao_community.farao.gridcapa.task_manager.api.*;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.Interval;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
class HourlyF303InfoGeneratorTest {

    private FbConstraint nativeCrac;
    private final Instant instantStart = Instant.parse("2023-08-21T15:16:00Z");
    private final Instant instantEnd = Instant.parse("2023-08-21T15:17:00Z");
    private final Interval interval = Interval.of(instantStart, instantEnd);
    private TaskDto taskDto;
    private MinioAdapter minioAdapter;
    private InputStream networkIS;
    private InputStream raoResultIS;

    @BeforeEach
    void setUp() throws FileNotFoundException {
        importCrac();
        minioAdapter = Mockito.mock(MinioAdapter.class);
    }

    private void importCrac() throws FileNotFoundException {
        Path cracPath = Paths.get(Objects.requireNonNull(getClass().getResource("/services/crac.xml")).getPath());
        File cracFile = new File(cracPath.toString());
        InputStream cracInputStream = new FileInputStream(cracFile);
        nativeCrac = new FbConstraintImporter().importNativeCrac(cracInputStream);
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
        taskDto = Utils.makeTask(TaskStatus.SUCCESS);
        Mockito.doReturn(networkIS).when(minioAdapter).getFile("network.uct");
        Mockito.doReturn(raoResultIS).when(minioAdapter).getFile("raoResult.json");
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter);
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
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, null, minioAdapter);
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate();
        checkCriticalBranches(hourlyF303Info);
    }

    @Test
    void generateForNotSuccessfulTask() {
        taskDto = Utils.makeTask(TaskStatus.ERROR);
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter);
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
