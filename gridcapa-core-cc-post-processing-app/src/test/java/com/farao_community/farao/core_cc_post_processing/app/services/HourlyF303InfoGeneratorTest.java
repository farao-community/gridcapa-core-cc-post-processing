/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.Utils;
import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileStatus;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.openrao.data.cracapi.parameters.CracCreationParameters;
import com.powsybl.openrao.data.cracapi.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.craccreation.creator.fbconstraint.xsd.FlowBasedConstraintDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.Interval;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;

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
        taskDto = Utils.SUCCESS_TASK;
        Mockito.doReturn(networkIS).when(minioAdapter).getFileFromFullPath("network.uct");
        Mockito.doReturn(raoResultIS).when(minioAdapter).getFileFromFullPath("raoResult.json");
        //crac creation parameters
        final CracCreationParameters cracCreationParameters = JsonCracCreationParameters.read(getClass().getResourceAsStream("/services/crac/cracCreationParameters.json"));
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter, cracCreationParameters);
        final ProcessFileDto processFileDto = new ProcessFileDto("raoResult.json", "", ProcessFileStatus.VALIDATED, "raoResult.json", OffsetDateTime.now());
        final ProcessFileDto cgmProcessFile = new ProcessFileDto("network.uct", "", ProcessFileStatus.VALIDATED, "network.uct", OffsetDateTime.now());
        //
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate(processFileDto, cgmProcessFile, "crac.xml", cracInputStream);
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
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate(null, null, null, null);
        checkCriticalBranches(hourlyF303Info);
    }

    @Test
    void generateForNotSuccessfulTask() {
        taskDto = Utils.ERROR_TASK;
        HourlyF303InfoGenerator hourlyF303InfoGenerator = new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter, new CracCreationParameters());
        HourlyF303Info hourlyF303Info = hourlyF303InfoGenerator.generate(null, null, "crac.xml", cracInputStream);
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

    FlowBasedConstraintDocument importNativeCrac(InputStream inputStream) {
        try {
            byte[] bytes = getBytesFromInputStream(inputStream);
            JAXBContext jaxbContext = JAXBContext.newInstance(FlowBasedConstraintDocument.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            FlowBasedConstraintDocument document = (FlowBasedConstraintDocument) jaxbUnmarshaller.unmarshal(new ByteArrayInputStream(bytes));
            return document;
        } catch (JAXBException | IOException e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during import of native crac", e);
        }
    }

    private static byte[] getBytesFromInputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        org.apache.commons.io.IOUtils.copy(inputStream, baos);
        return baos.toByteArray();
    }
}
