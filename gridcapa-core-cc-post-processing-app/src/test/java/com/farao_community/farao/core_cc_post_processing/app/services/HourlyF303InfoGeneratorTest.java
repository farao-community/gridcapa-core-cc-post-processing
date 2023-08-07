/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

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
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class HourlyF303InfoGeneratorTest {

    private FbConstraint nativeCrac;
    private Instant instantStart = Instant.parse("2023-08-07T14:50:05Z");
    private Instant instantEnd = Instant.parse("2023-08-07T14:51:00Z");
    private Interval interval = Interval.of(instantStart, instantEnd);
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
        Path cracPath = Paths.get(getClass().getResource("/services/crac.xml").getPath());
        File cracFile = new File(cracPath.toString());
        InputStream cracInputStream = new FileInputStream(cracFile);
        nativeCrac = new FbConstraintImporter().importNativeCrac(cracInputStream);
    }

    private void importNetwork() throws FileNotFoundException {
        Path networkPath = Paths.get(getClass().getResource("/services/network.uct").getPath());
        File networkFile = new File(networkPath.toString());
        networkIS = new FileInputStream(networkFile);
    }

    private void importRaoResult() throws FileNotFoundException {
        Path raoResultPath = Paths.get(getClass().getResource("/services/raoResult.json").getPath());
        File raoResultFile = new File(raoResultPath.toString());
        raoResultIS = new FileInputStream(raoResultFile);
    }

    private void initTask() {
        UUID uuid = UUID.fromString("22711acb-ee59-47ed-b877-3c3688efe820");
        OffsetDateTime dateTime = OffsetDateTime.of(2023, 8, 7, 16, 30, 00, 0, ZoneId.of("Europe/Brussels").getRules().getOffset(LocalDateTime.now()));
        List<ProcessFileDto> inputs = List.of();
        List<ProcessFileDto> outputs = initTaskOutputs(dateTime);
        List<ProcessEventDto> processEvents = List.of();
        taskDto = new TaskDto(uuid, dateTime, TaskStatus.SUCCESS, inputs, outputs, processEvents);
    }

    private List<ProcessFileDto> initTaskOutputs(OffsetDateTime dateTime) {
        ProcessFileDto raoResultFileDto = new ProcessFileDto("/CORE/CC/raoResult.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "cne.xml", dateTime);
        ProcessFileDto cgmFileDto = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", dateTime);
        return List.of(raoResultFileDto, cgmFileDto);
    }

    @Test
    void generate() throws FileNotFoundException {
        importNetwork();
        importRaoResult();
        initTask();
        Mockito.doReturn(networkIS).when(minioAdapter).getFile(Mockito.eq("network.uct"));
        Mockito.doReturn(raoResultIS).when(minioAdapter).getFile(Mockito.eq("raoResult.json"));
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter);
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate();
        checkCriticalBranchesWithTatlPatl(hourlyF303Info);
        checkComplexVariants(hourlyF303Info);
    }

    private static void checkComplexVariants(HourlyF303Info hourlyF303Info) {
        assertEquals(1, hourlyF303Info.getComplexVariants().size());
        assertEquals("CRA_160001", hourlyF303Info.getComplexVariants().get(0).getId());
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
        taskDto = Mockito.mock(TaskDto.class);
        Mockito.doReturn(TaskStatus.ERROR).when(taskDto).getStatus();
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
