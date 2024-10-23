/*
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.cracio.fbconstraint.xsd.CriticalBranchType;
import com.powsybl.openrao.data.cracio.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.powsybl.openrao.data.cracio.fbconstraint.xsd.IndependantComplexVariant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Thomas Bouquet {@literal  <thomas.bouquet at rte-france.com>}
 */

@SpringBootTest
class DailyF303Generator1Test {

    /*
     * test-case initially designed to test the F303 export, with two hours of data, one contingency
     * and some elements which are not CNEC and not MNEC
     */

    @Autowired
    private DailyF303Generator dailyF303Generator;

    @MockBean
    private MinioAdapter minioAdapter;
    private final Set<TaskDto> taskDtos = new HashSet<>();
    private final Map<TaskDto, ProcessFileDto> raoResult = new HashMap<>();
    private final Map<TaskDto, ProcessFileDto> cgms = new HashMap<>();

    @BeforeEach
    public void setUp() {
        InputStream inputCracXmlInputStream = getClass().getResourceAsStream("/services/f303-1/inputs/F301.xml");
        Mockito.doReturn(inputCracXmlInputStream).when(minioAdapter).getFileFromFullPath("/CORE/CC/inputCracXml.xml");

        InputStream network1InputStream = getClass().getResourceAsStream("/services/f303-1/inputs/networks/20190108_1230.xiidm");
        Mockito.doReturn(network1InputStream).when(minioAdapter).getFileFromFullPath("/CORE/CC/network1.xiidm");

        InputStream raoResult1InputStream = getClass().getResourceAsStream("/services/f303-1/hourly_rao_results/20190108_1230/raoResult.json");
        Mockito.doReturn(raoResult1InputStream).when(minioAdapter).getFileFromFullPath("/CORE/CC/raoResult1.json");

        InputStream network2InputStream = getClass().getResourceAsStream("/services/f303-1/inputs/networks/20190108_1330.xiidm");
        Mockito.doReturn(network2InputStream).when(minioAdapter).getFileFromFullPath("/CORE/CC/network2.xiidm");

        InputStream raoResult2InputStream = getClass().getResourceAsStream("/services/f303-1/hourly_rao_results/20190108_1330/raoResult.json");
        Mockito.doReturn(raoResult2InputStream).when(minioAdapter).getFileFromFullPath("/CORE/CC/raoResult2.json");

        String timeStampBegin = "2019-01-07T23:00Z";

        // tasks data
        String baseUuid = "5bec38f9-80c6-4441-bbe7-b9dca13ca2";
        OffsetDateTime firstTimestamp = OffsetDateTime.parse(timeStampBegin);
        ProcessFileDto cracProcessFile = new ProcessFileDto("/CORE/CC/inputCracXml.xml", "CBCORA", ProcessFileStatus.VALIDATED, "inputCracXml.xml", firstTimestamp);

        // NOT_CREATED tasks from 2019-01-07 23:00 to 2019-01-08 11:00
        for (int h = 0; h <= 12; h++) {
            // Set tasks' status to NOT_CREATED to ignore them
            OffsetDateTime timestamp = firstTimestamp.plusHours(h);
            taskDtos.add(new TaskDto(UUID.fromString(baseUuid + h), timestamp, TaskStatus.NOT_CREATED, List.of(cracProcessFile), List.of(), List.of(), List.of(), List.of(), List.of()));
        }

        // SUCCESS task at 12:30
        OffsetDateTime timestamp1230 = OffsetDateTime.parse("2019-01-08T12:30:00Z");
        ProcessFileDto cgm1ProcessFile = new ProcessFileDto("/CORE/CC/network1.xiidm", "CGM_OUT", ProcessFileStatus.VALIDATED, "network1.xiidm", timestamp1230);
        ProcessFileDto raoResult1ProcessFile = new ProcessFileDto("/CORE/CC/raoResult1.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult1.json", timestamp1230);
        final TaskDto successTaskOne = new TaskDto(UUID.fromString(baseUuid + 13), timestamp1230, TaskStatus.SUCCESS, List.of(cracProcessFile), List.of(cgm1ProcessFile, raoResult1ProcessFile), List.of(), List.of(), List.of(), List.of());
        taskDtos.add(successTaskOne);

        // SUCCESS task at 13:30
        OffsetDateTime timestamp1330 = OffsetDateTime.parse("2019-01-08T13:30:00Z");
        ProcessFileDto cgm2ProcessFile = new ProcessFileDto("/CORE/CC/network2.xiidm", "CGM_OUT", ProcessFileStatus.VALIDATED, "network2.xiidm", timestamp1330);
        ProcessFileDto raoResult2ProcessFile = new ProcessFileDto("/CORE/CC/raoResult2.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult2.json", timestamp1330);
        final TaskDto successTaskTwo = new TaskDto(UUID.fromString(baseUuid + 14), timestamp1330, TaskStatus.SUCCESS, List.of(cracProcessFile), List.of(cgm2ProcessFile, raoResult2ProcessFile), List.of(), List.of(), List.of(), List.of());
        taskDtos.add(successTaskTwo);

        raoResult.put(successTaskOne, raoResult1ProcessFile);
        raoResult.put(successTaskTwo, raoResult2ProcessFile);
        cgms.put(successTaskOne, cgm1ProcessFile);
        cgms.put(successTaskTwo, cgm2ProcessFile);

        // NOT_CREATED tasks from 2019-01-08 14:00 to 2019-01-08 23:00
        for (int h = 15; h <= 23; h++) {
            // Set tasks' status to NOT_CREATED to ignore them
            OffsetDateTime timestamp = firstTimestamp.plusHours(h);
            taskDtos.add(new TaskDto(UUID.fromString(baseUuid + h), timestamp, TaskStatus.NOT_CREATED, List.of(cracProcessFile), List.of(), List.of(), List.of(), List.of(), List.of()));
        }
    }

    @Test
    void validateMergedFlowBasedCreation() {
        assertEquals(24, taskDtos.size());
        FlowBasedConstraintDocument dailyFbConstDocument = dailyF303Generator.generate(raoResult, cgms);
        assertDocumentProperties(dailyFbConstDocument);
        assertCriticalBranches(dailyFbConstDocument.getCriticalBranches().getCriticalBranch());
        assertComplexVariants(dailyFbConstDocument.getComplexVariants().getComplexVariant());
    }

    private void assertDocumentProperties(FlowBasedConstraintDocument document) {
        assertEquals("22XCORESO------S-20190108-F303v1", document.getDocumentIdentification().getV());
        assertEquals(1, document.getDocumentVersion().getV());
        assertEquals("B07", document.getDocumentType().getV().value());
        assertEquals("22XCORESO------S", document.getSenderIdentification().getV());
        assertEquals("A44", document.getSenderRole().getV().value());
        assertEquals("17XTSO-CS------W", document.getReceiverIdentification().getV());
        assertEquals("A36", document.getReceiverRole().getV().value());
        assertEquals("2019-01-07T23:00Z/2019-01-08T23:00Z", document.getConstraintTimeInterval().getV());
        assertEquals("10YDOM-REGION-1V", document.getDomain().getV());
    }

    private void assertCriticalBranches(List<CriticalBranchType> criticalBranches) {
        assertEquals(15, criticalBranches.size());
        assertCriticalBranch(criticalBranches, "de2_nl3_N", "2019-01-08T12:00Z/2019-01-08T14:00Z");
        assertCriticalBranch(criticalBranches, "fr1_fr4_CO1_PATL", "2019-01-08T12:00Z/2019-01-08T13:00Z");
        assertCriticalBranch(criticalBranches, "fr1_fr4_CO1_TATL", "2019-01-08T12:00Z/2019-01-08T14:00Z");
    }

    private void assertCriticalBranch(List<CriticalBranchType> criticalBranches, String branchId, String timeInterval) {
        CriticalBranchType branch = findBranchById(criticalBranches, branchId);
        assertNotNull(branch);
        assertEquals(timeInterval, branch.getTimeInterval().getV());
    }

    private CriticalBranchType findBranchById(List<CriticalBranchType> criticalBranches, String branchId) {
        return criticalBranches.stream()
                .filter(cb -> cb.getId().equals(branchId))
                .findFirst()
                .orElse(null);
    }

    private void assertComplexVariants(List<IndependantComplexVariant> complexVariants) {
        assertEquals(2, complexVariants.size());
        assertComplexVariant(complexVariants.get(0), "open_fr1_fr3;pst_be", "2019-01-08T12:00Z/2019-01-08T13:00Z");
        assertComplexVariant(complexVariants.get(1), "open_fr1_fr3;pst_be", "2019-01-08T13:00Z/2019-01-08T14:00Z");
    }

    private void assertComplexVariant(IndependantComplexVariant complexVariant, String name, String timeInterval) {
        assertEquals(name, complexVariant.getName());
        assertEquals(timeInterval, complexVariant.getTimeInterval().getV());
        assertEquals("XX", complexVariant.getTsoOrigin());
        assertEquals(2, complexVariant.getActionsSet().size());
    }

}

