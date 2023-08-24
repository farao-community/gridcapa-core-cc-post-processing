/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.*;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

    @BeforeEach
    public void setUp() {
        InputStream inputCracXmlInputStream = getClass().getResourceAsStream("/services/f303-1/inputs/F301.xml");
        Mockito.doReturn(inputCracXmlInputStream).when(minioAdapter).getFile("inputCracXml.xml");

        InputStream network1InputStream = getClass().getResourceAsStream("/services/f303-1/inputs/networks/20190108_1230.xiidm");
        Mockito.doReturn(network1InputStream).when(minioAdapter).getFile("network1.xiidm");
        InputStream raoResult1InputStream = getClass().getResourceAsStream("/services/f303-1/hourly_rao_results/20190108_1230/raoResult.json");
        Mockito.doReturn(raoResult1InputStream).when(minioAdapter).getFile("raoResult1.json");

        InputStream network2InputStream = getClass().getResourceAsStream("/services/f303-1/inputs/networks/20190108_1330.xiidm");
        Mockito.doReturn(network2InputStream).when(minioAdapter).getFile("network2.xiidm");
        InputStream raoResult2InputStream = getClass().getResourceAsStream("/services/f303-1/hourly_rao_results/20190108_1330/raoResult.json");
        Mockito.doReturn(raoResult2InputStream).when(minioAdapter).getFile("raoResult2.json");

        String timeStampBegin = "2019-01-07T23:00Z";

        // tasks data
        String baseUuid = "5bec38f9-80c6-4441-bbe7-b9dca13ca2";
        OffsetDateTime firstTimestamp = OffsetDateTime.parse(timeStampBegin);
        ProcessFileDto cracProcessFile = new ProcessFileDto("/CORE/CC/inputCracXml.xml", "CBCORA", ProcessFileStatus.VALIDATED, "inputCracXml.xml", firstTimestamp);

        // NOT_CREATED tasks from 2019-01-07 23:00 to 2019-01-08 11:00
        for (int h = 0; h <= 12; h++) {
            // Set tasks' status to NOT_CREATED to ignore them
            OffsetDateTime timestamp = firstTimestamp.plusHours(h);
            taskDtos.add(new TaskDto(UUID.fromString(baseUuid + h), timestamp, TaskStatus.NOT_CREATED, List.of(cracProcessFile), List.of(), List.of()));
        }

        // SUCCESS task at 12:30
        OffsetDateTime timestamp1230 = OffsetDateTime.parse("2019-01-08T12:30:00Z");
        ProcessFileDto cgm1ProcessFile = new ProcessFileDto("/CORE/CC/network1.xiidm", "CGM_OUT", ProcessFileStatus.VALIDATED, "network1.xiidm", timestamp1230);
        ProcessFileDto raoResult1ProcessFile = new ProcessFileDto("/CORE/CC/raoResult1.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult1.json", timestamp1230);
        taskDtos.add(new TaskDto(UUID.fromString(baseUuid + 13), timestamp1230, TaskStatus.SUCCESS, List.of(cracProcessFile), List.of(cgm1ProcessFile, raoResult1ProcessFile), List.of()));

        // SUCCESS task at 13:30
        OffsetDateTime timestamp1330 = OffsetDateTime.parse("2019-01-08T13:30:00Z");
        ProcessFileDto cgm2ProcessFile = new ProcessFileDto("/CORE/CC/network2.xiidm", "CGM_OUT", ProcessFileStatus.VALIDATED, "network2.xiidm", timestamp1330);
        ProcessFileDto raoResult2ProcessFile = new ProcessFileDto("/CORE/CC/raoResult2.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult2.json", timestamp1330);
        taskDtos.add(new TaskDto(UUID.fromString(baseUuid + 14), timestamp1330, TaskStatus.SUCCESS, List.of(cracProcessFile), List.of(cgm2ProcessFile, raoResult2ProcessFile), List.of()));

        // NOT_CREATED tasks from 2019-01-08 14:00 to 2019-01-08 23:00
        for (int h = 15; h <= 23; h++) {
            // Set tasks' status to NOT_CREATED to ignore them
            OffsetDateTime timestamp = firstTimestamp.plusHours(h);
            taskDtos.add(new TaskDto(UUID.fromString(baseUuid + h), timestamp, TaskStatus.NOT_CREATED, List.of(cracProcessFile), List.of(), List.of()));
        }
    }

    @Test
    void validateMergedFlowBasedCreation() {
        assertEquals(24, taskDtos.size());
        FlowBasedConstraintDocument dailyFbConstDocument = dailyF303Generator.generate(taskDtos);
        assertEquals("22XCORESO------S-20190108-F303v1", dailyFbConstDocument.getDocumentIdentification().getV());
        assertEquals(1, dailyFbConstDocument.getDocumentVersion().getV());
        assertEquals("B07", dailyFbConstDocument.getDocumentType().getV().value());
        assertEquals("22XCORESO------S", dailyFbConstDocument.getSenderIdentification().getV());
        assertEquals("A44", dailyFbConstDocument.getSenderRole().getV().value());
        assertEquals("17XTSO-CS------W", dailyFbConstDocument.getReceiverIdentification().getV());
        assertEquals("A36", dailyFbConstDocument.getReceiverRole().getV().value());
        assertEquals("2019-01-07T23:00Z/2019-01-08T23:00Z", dailyFbConstDocument.getConstraintTimeInterval().getV());
        assertEquals("10YDOM-REGION-1V", dailyFbConstDocument.getDomain().getV());
        assertEquals(23, dailyFbConstDocument.getCriticalBranches().getCriticalBranch().size());
        List<CriticalBranchType> criticalBranchTypes = dailyFbConstDocument.getCriticalBranches().getCriticalBranch();
        List<CriticalBranchType> fr1Fr4CO1 = criticalBranchTypes.stream().filter(cb -> cb.getId().equals("fr1_fr4_CO1")).collect(Collectors.toList());
        assertEquals("2019-01-07T23:00Z/2019-01-08T12:00Z", fr1Fr4CO1.get(0).getTimeInterval().getV());
        assertEquals("2019-01-08T14:00Z/2019-01-08T23:00Z", fr1Fr4CO1.get(1).getTimeInterval().getV());
        List<CriticalBranchType> fr1Fr4Co1Patl = criticalBranchTypes.stream().filter(cb -> cb.getId().equals("fr1_fr4_CO1_PATL")).collect(Collectors.toList());
        assertEquals("2019-01-08T12:00Z/2019-01-08T13:00Z", fr1Fr4Co1Patl.get(0).getTimeInterval().getV());
        assertEquals("fr1_fr4_CO1", fr1Fr4Co1Patl.get(0).getOriginalId());
        assertEquals("2019-01-08T13:00Z/2019-01-08T14:00Z", fr1Fr4Co1Patl.get(1).getTimeInterval().getV());
        assertEquals("fr1_fr4_CO1", fr1Fr4Co1Patl.get(1).getOriginalId());
        assertNotEquals(fr1Fr4Co1Patl.get(0).getComplexVariantId(), fr1Fr4Co1Patl.get(1).getComplexVariantId());
        assertEquals("1", fr1Fr4Co1Patl.get(0).getImaxFactor().toString());
        assertNull(fr1Fr4Co1Patl.get(0).getImaxA());

        List<CriticalBranchType> fr1Fr4Co1Tatl = criticalBranchTypes.stream().filter(cb -> cb.getId().equals("fr1_fr4_CO1_TATL")).collect(Collectors.toList());
        assertEquals("2019-01-08T12:00Z/2019-01-08T14:00Z", fr1Fr4Co1Tatl.get(0).getTimeInterval().getV());
        assertEquals("fr1_fr4_CO1", fr1Fr4Co1Tatl.get(0).getOriginalId());
        assertEquals("1000", fr1Fr4Co1Tatl.get(0).getImaxFactor().toString());
        assertNull(fr1Fr4Co1Tatl.get(0).getImaxA());
        assertNull(fr1Fr4Co1Tatl.get(0).getComplexVariantId());

        assertEquals(2, dailyFbConstDocument.getComplexVariants().getComplexVariant().size());
        assertEquals("open_fr1_fr3;pst_be", dailyFbConstDocument.getComplexVariants().getComplexVariant().get(0).getName());
        assertEquals("2019-01-08T12:00Z/2019-01-08T13:00Z", dailyFbConstDocument.getComplexVariants().getComplexVariant().get(0).getTimeInterval().getV());
        assertEquals("open_fr1_fr3;pst_be", dailyFbConstDocument.getComplexVariants().getComplexVariant().get(1).getName());
        assertEquals("2019-01-08T13:00Z/2019-01-08T14:00Z", dailyFbConstDocument.getComplexVariants().getComplexVariant().get(1).getTimeInterval().getV());

        assertEquals("XX", dailyFbConstDocument.getComplexVariants().getComplexVariant().get(1).getTsoOrigin());
        List<ActionsSetType> actionsSetTypeList = dailyFbConstDocument.getComplexVariants().getComplexVariant().get(1).getActionsSet();
        assertEquals(2, actionsSetTypeList.size());
        assertEquals("open_fr1_fr3", actionsSetTypeList.get(0).getName());
        assertTrue(actionsSetTypeList.get(0).isCurative());
        assertFalse(actionsSetTypeList.get(0).isPreventive());
        List<String> afterCOListList = actionsSetTypeList.get(0).getAfterCOList().getAfterCOId();
        assertEquals(1, afterCOListList.size());
        assertEquals("CO1_fr2_fr3_1", afterCOListList.get(0));
        List<ActionType> actionTypeList = actionsSetTypeList.get(0).getAction();
        assertEquals("STATUS", actionTypeList.get(0).getType());

        assertEquals("pst_be", actionsSetTypeList.get(1).getName());
        assertTrue(actionsSetTypeList.get(1).isCurative());
        assertFalse(actionsSetTypeList.get(0).isPreventive());
        List<String> afterCOListList1 = actionsSetTypeList.get(1).getAfterCOList().getAfterCOId();
        assertEquals(1, afterCOListList1.size());
        assertEquals("CO1_fr2_fr3_1", afterCOListList1.get(0));
        List<ActionType> actionTypeList1 = actionsSetTypeList.get(1).getAction();
        assertEquals("PSTTAP", actionTypeList1.get(0).getType());
    }

}

