/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.gridcapa.task_manager.api.*;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class DailyF303GeneratorTest {

    private MinioAdapter minioAdapter = Mockito.mock(MinioAdapter.class);
    private TaskDto taskDto;
    private Set<TaskDto> taskDtos;
    private InputStream cracIS;
    private InputStream networkIS;
    private InputStream raoResultIS;

    @Test
    void generate() throws FileNotFoundException {
        initTask();
        importFiles();
        Mockito.doReturn(cracIS).when(minioAdapter).getFile(Mockito.eq("crac.xml"));
        Mockito.doReturn(networkIS).when(minioAdapter).getFile(Mockito.eq("network.uct"));
        Mockito.doReturn(raoResultIS).when(minioAdapter).getFile(Mockito.eq("raoResult.json"));
        DailyF303Generator dailyF303Generator = new DailyF303Generator(minioAdapter);
        FlowBasedConstraintDocument flowBasedConstraintDocument = dailyF303Generator.generate(taskDtos);
    }

    private void initTask() {
        UUID uuid = UUID.fromString("22711acb-ee59-47ed-b877-3c3688efe820");
        OffsetDateTime dateTime = OffsetDateTime.of(2023, 8, 7, 16, 30, 00, 0, ZoneId.of("Europe/Brussels").getRules().getOffset(LocalDateTime.now()));
        ProcessFileDto cracProcessFile = new ProcessFileDto("/CORE/CC/crac.xml", "CBCORA", ProcessFileStatus.VALIDATED, "crac.xml", dateTime);
        ProcessFileDto raoResultFileDto = new ProcessFileDto("/CORE/CC/raoResult.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "cne.xml", dateTime);
        ProcessFileDto cgmFileDto = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", dateTime);
        List<ProcessFileDto> inputs = List.of(cracProcessFile);
        List<ProcessFileDto> outputs = List.of(raoResultFileDto, cgmFileDto);
        List<ProcessEventDto> processEvents = List.of();
        taskDto = new TaskDto(uuid, dateTime, TaskStatus.SUCCESS, inputs, outputs, processEvents);
        taskDtos = Set.of(taskDto);
    }

    private void importFiles() throws FileNotFoundException {
        importCrac();
        importNetwork();
        importRaoResult();
    }

    private void importCrac() throws FileNotFoundException {
        Path cracPath = Paths.get(getClass().getResource("/services/crac.xml").getPath());
        File cracFile = new File(cracPath.toString());
        cracIS = new FileInputStream(cracFile);
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
}
