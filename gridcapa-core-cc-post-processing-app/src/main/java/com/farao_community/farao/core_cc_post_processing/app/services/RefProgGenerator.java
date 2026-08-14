/*
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.configuration.PiSaConfiguration;
import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.iidm.network.Country;
import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.commons.TemporalData;
import com.powsybl.openrao.commons.TemporalDataImpl;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.CracCreationContext;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.parameters.JsonCracCreationParameters;
import com.powsybl.openrao.data.crac.api.rangeaction.InjectionRangeAction;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintImporter;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import com.powsybl.openrao.data.refprog.refprogxmlimporter.RefProgImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * @author Vincent Bochet {@literal <vincent.bochet at rte-france.com>}
 */
@Service
public class RefProgGenerator {

    public static final String CRAC_CREATION_PARAMETERS_JSON = "/crac/cracCreationParameters.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(RefProgGenerator.class);
    private final MinioAdapter minioAdapter;
    private final PiSaLinkProcessor piSaLink1Processor;
    private final PiSaLinkProcessor piSaLink2Processor;

    public RefProgGenerator(final MinioAdapter minioAdapter,
                            final PiSaConfiguration.PiSaLinkConfiguration piSaLink1Configuration,
                            final PiSaConfiguration.PiSaLinkConfiguration piSaLink2Configuration) {
        this.minioAdapter = minioAdapter;
        this.piSaLink1Processor = new PiSaLinkProcessor(piSaLink1Configuration);
        this.piSaLink2Processor = new PiSaLinkProcessor(piSaLink2Configuration);
    }

    public ByteArrayOutputStream generate(final Set<TaskDto> tasksToPostProcess, Map<TaskDto, ProcessFileDto> cgms, Map<TaskDto, ProcessFileDto> raoResults) {
        // TODO :
        //  - Pour chaque timestamp :
        //    - Récupérer le CGM pour pouvoir reconstruire le CRAC
        //    - Récupérer le CRAC pour récupérer le PreventiveState, les RangeAction correspondant à la HVDC PiSA
        //    - Récupérer le setpoint initial de FR-PISA (somme des initialSetPoint des RangeActions FR-PiSA ?)
        //    - Récupérer le RaoResult pour récupérer les optimizedSetPointOnState de chaque RA PiSA en préventif (et éventuellement isActiveDuringState)
        //  - Récupérer le RefProg initial
        //  - Pour chaque timestamp :
        //    - Si nécessaire, mettre à jour les valeurs des quatre setpoint PiSA dans le RefProg avec les données obtenues du RaoResult
        //    - De même, mettre à jour la valeur d'échange FR-IT avec les valeurs du RaoResult et du CGM

        final CracCreationParameters cracCreationParameters = getCimCracCreationParameters();
        final TaskDto firstTasksToPostProcess = tasksToPostProcess.stream().findFirst().orElseThrow();
        final ProcessFileDto cracFileDto = getInputFile(firstTasksToPostProcess, "CBCORA");
        final ProcessFileDto refProgFileDto = getInputFile(firstTasksToPostProcess, "REFPROG");

        final TemporalData<Double> initialFrPisaSetpoints = new TemporalDataImpl<>();
        final TemporalData<Map<String, Double>> postPraPisaSetpoints = new TemporalDataImpl<>();

        for (TaskDto task : tasksToPostProcess) {
            final OffsetDateTime timestamp = task.getTimestamp();

            final ProcessFileDto cgmFileDto = cgms.get(task);
            final ProcessFileDto raoResultFileDto = raoResults.get(task);

            final Network network = getNetwork(cgmFileDto, timestamp);
            final Crac crac = getCrac(cracCreationParameters, cracFileDto, network, timestamp);
            final RaoResult raoResult = getRaoResult(crac, raoResultFileDto, timestamp);

            final State preventiveState = crac.getPreventiveState();

            final Set<InjectionRangeAction> piSaLink1RangeActions = piSaLink1Processor.getRelatedHvdcRangeActions(crac);
            final Set<InjectionRangeAction> piSaLink2RangeActions = piSaLink2Processor.getRelatedHvdcRangeActions(crac);

            final AtomicReference<Double> initialFrPisaSetpoint = new AtomicReference<>(0.);
            final Map<String, Double> setpointsAfterPra = new HashMap<>();
            Stream.concat(piSaLink1RangeActions.stream(), piSaLink2RangeActions.stream())
                .forEach(rangeAction -> {
                    if (rangeAction.getLocation(network).contains(Country.FR)) {
                        initialFrPisaSetpoint.updateAndGet(v -> v + rangeAction.getInitialSetpoint());
                    }

//                    if (raoResult.isActivatedDuringState(preventiveState, rangeAction)) {
                    final double optimizedSetPointOnState = raoResult.getOptimizedSetPointOnState(preventiveState, rangeAction);
                    setpointsAfterPra.put(rangeAction.getId(), optimizedSetPointOnState);
//                    }
                });

            initialFrPisaSetpoints.put(timestamp, initialFrPisaSetpoint.get());
            postPraPisaSetpoints.put(timestamp, setpointsAfterPra);
        }

        try (final InputStream refProgInputStream = minioAdapter.getFileFromFullPath(refProgFileDto.getFilePath())) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            RefProgImporter.updateRefProg(refProgInputStream, initialFrPisaSetpoints, postPraPisaSetpoints, outputStream);
            return outputStream;
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot generate updated RefProg for business date %s", LocalDate.from(firstTasksToPostProcess.getTimestamp())), e);
        }
    }

    private CracCreationParameters getCimCracCreationParameters() {
        LOGGER.info("Importing Crac Creation Parameters file: {}", CRAC_CREATION_PARAMETERS_JSON);
        return JsonCracCreationParameters.read(getClass().getResourceAsStream(CRAC_CREATION_PARAMETERS_JSON));
    }

    private static ProcessFileDto getInputFile(final TaskDto task, final String type) {
        return task.getInputs().stream()
            .filter(processFileDto -> processFileDto.getFileType().equals(type))
            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException("task dto missing " + type + " file"));
    }

    private Network getNetwork(final ProcessFileDto cgmProcessFile, final OffsetDateTime timestamp) {
        try (InputStream networkInputStream = minioAdapter.getFileFromFullPath(cgmProcessFile.getFilePath())) {
            return Network.read(cgmProcessFile.getFilename(), networkInputStream);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import network of task %s", timestamp), e);
        }
    }

    public Crac getCrac(final CracCreationParameters cracCreationParameters,
                        final ProcessFileDto cracFileDto,
                        final Network network,
                        final OffsetDateTime timestamp) {
        try (final InputStream cracXmlInputStream = minioAdapter.getFileFromFullPath(cracFileDto.getFilePath())) {
            final CracCreationContext cracCreationContext = new FbConstraintImporter().importData(cracXmlInputStream, cracCreationParameters, network);
            return cracCreationContext.getCrac();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException(String.format("Exception occurred while importing CRAC file for task %s", timestamp), e);
        }
    }

    private RaoResult getRaoResult(final Crac crac,
                                   final ProcessFileDto raoResultProcessFile,
                                   final OffsetDateTime timestamp) {
        try (InputStream raoResultInputStream = minioAdapter.getFileFromFullPath(raoResultProcessFile.getFilePath())) {
            return RaoResult.read(raoResultInputStream, crac);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import RAO result of hourly RAO response of instant %s", timestamp), e);
        }
    }
}
