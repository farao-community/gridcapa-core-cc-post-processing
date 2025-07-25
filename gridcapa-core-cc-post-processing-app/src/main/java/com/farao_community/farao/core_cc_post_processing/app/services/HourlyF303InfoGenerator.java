/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskStatus;
import com.farao_community.farao.gridcapa_core_cc.api.exception.CoreCCInternalException;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import com.powsybl.iidm.network.Network;
import com.powsybl.openrao.data.crac.api.Crac;
import com.powsybl.openrao.data.crac.api.CracCreationContext;
import com.powsybl.openrao.data.crac.api.RemedialAction;
import com.powsybl.openrao.data.crac.api.State;
import com.powsybl.openrao.data.crac.api.networkaction.NetworkAction;
import com.powsybl.openrao.data.crac.api.parameters.CracCreationParameters;
import com.powsybl.openrao.data.crac.api.rangeaction.PstRangeAction;
import com.powsybl.openrao.data.crac.api.rangeaction.RangeAction;
import com.powsybl.openrao.data.crac.io.commons.api.ImportStatus;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintCreationContext;
import com.powsybl.openrao.data.crac.io.fbconstraint.FbConstraintImporter;
import com.powsybl.openrao.data.crac.io.fbconstraint.parameters.FbConstraintCracCreationParameters;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.ActionType;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.CriticalBranchType;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.FlowBasedConstraintDocument;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.IndependantComplexVariant;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.ObjectFactory;
import com.powsybl.openrao.data.crac.io.fbconstraint.xsd.etso.TimeIntervalType;
import com.powsybl.openrao.data.raoresult.api.RaoResult;
import org.threeten.extra.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
class HourlyF303InfoGenerator {

    private static final String TATL = "_TATL";
    private static final String PATL = "_PATL";

    private final FlowBasedConstraintDocument flowBasedConstraintDocument;
    private final Interval interval;
    private final TaskDto taskDto;
    private final MinioAdapter minioAdapter;
    private final CracCreationParameters cracCreationParameters;

    HourlyF303InfoGenerator(FlowBasedConstraintDocument flowBasedConstraintDocument, Interval interval, TaskDto taskDto, MinioAdapter minioAdapter, CracCreationParameters cracCreationParameters) {
        this.flowBasedConstraintDocument = flowBasedConstraintDocument;
        this.interval = interval;
        this.taskDto = taskDto;
        this.minioAdapter = minioAdapter;
        this.cracCreationParameters = cracCreationParameters;
    }

    HourlyF303Info generate(ProcessFileDto raoResultProcessFile, ProcessFileDto cgmProcessFile, InputStream cracInputStream) {
        if (taskDto == null || !taskDto.getStatus().equals(TaskStatus.SUCCESS)) {
            return getInfoForNonRequestedOrFailedInterval();
        }
        if (raoResultProcessFile == null) {
            throw new CoreCCInternalException(String.format("raoResult is null although task status is %s", taskDto.getStatus().toString()));
        }
        return getInfoForSuccessfulInterval(raoResultProcessFile, cgmProcessFile, cracInputStream);
    }

    private HourlyF303Info getInfoForNonRequestedOrFailedInterval() {
        OffsetDateTime startTime = OffsetDateTime.ofInstant(interval.getStart(), ZoneOffset.UTC);
        List<CriticalBranchType> criticalBranches = new ArrayList<>();
        TimeIntervalType ti = new TimeIntervalType();
        ti.setV(IntervalUtil.getCurrentTimeInterval(startTime));

        flowBasedConstraintDocument.getCriticalBranches().getCriticalBranch().stream()
                .filter(cb -> IntervalUtil.isInTimeInterval(startTime, cb.getTimeInterval().getV()))
                .forEach(refCb -> {
                    CriticalBranchType clonedCb = (CriticalBranchType) refCb.clone();
                    clonedCb.setTimeInterval(ti);
                    setPermanentLimit(clonedCb);
                    criticalBranches.add(clonedCb);
                });

        return new HourlyF303Info(criticalBranches);
    }

    private HourlyF303Info getInfoForSuccessfulInterval(ProcessFileDto raoResultProcessFile, ProcessFileDto cgmProcessFile, InputStream cracInputStream) {
        Network network = getNetworkOfTaskDto(cgmProcessFile);
        cracCreationParameters.addExtension(FbConstraintCracCreationParameters.class, new FbConstraintCracCreationParameters());
        cracCreationParameters.getExtension(FbConstraintCracCreationParameters.class).setTimestamp(taskDto.getTimestamp());
        FbConstraintCreationContext cracCreationContext = (FbConstraintCreationContext) new FbConstraintImporter().importData(cracInputStream, cracCreationParameters, network);
        RaoResult raoResult = getRaoResultOfTaskDto(cracCreationContext.getCrac(), raoResultProcessFile);

        Map<State, String> statesWithCra = getUIDOfStatesWithCra(cracCreationContext, raoResult, taskDto.getTimestamp().toString());

        List<CriticalBranchType> criticalBranches = getCriticalBranchesOfSuccessfulInterval(cracCreationContext, statesWithCra);
        List<IndependantComplexVariant> complexVariants = getComplexVariantsOfSuccesfulInterval(cracCreationContext, raoResult, statesWithCra);

        return new HourlyF303Info(criticalBranches, complexVariants);
    }

    private List<CriticalBranchType> getCriticalBranchesOfSuccessfulInterval(FbConstraintCreationContext cracCreationContext, Map<State, String> statesWithCrac) {
        TimeIntervalType ti = new TimeIntervalType();
        ti.setV(IntervalUtil.getCurrentTimeInterval(OffsetDateTime.ofInstant(interval.getStart(), ZoneOffset.UTC)));
        List<String> contingencyWithCra = statesWithCrac.keySet().stream().map(s -> s.getContingency().orElseThrow().getId()).toList();
        Map<String, CriticalBranchType> refCbs = getCriticalBranchesForInstant(cracCreationContext.getTimeStamp(), flowBasedConstraintDocument);
        List<CriticalBranchType> criticalBranches = new ArrayList<>();

        cracCreationContext.getBranchCnecCreationContexts().forEach(bccc -> {
            CriticalBranchType refCb = refCbs.get(bccc.getNativeObjectId());
            if (bccc.getImportStatus() != ImportStatus.NOT_FOR_REQUESTED_TIMESTAMP) {
                if (refCb.getOutage() == null || !contingencyWithCra.contains(refCb.getOutage().getId())) {

                    // N critical branch or N-1 critical branch without CRA
                    // -> export one critical branch with permanent limit and no associated variant
                    CriticalBranchType patlCb = (CriticalBranchType) refCb.clone();
                    patlCb.setTimeInterval(ti);
                    setPermanentLimit(patlCb);
                    criticalBranches.add(patlCb);

                } else {
                    // N-1 critical branch with CRA
                    // -> export one critical branch with temporary limit and no associated variant (OUTAGE)
                    // -> export one critical branch with permanent limit and associated variant (CURATIVE)

                    CriticalBranchType tatlCb = (CriticalBranchType) refCb.clone();
                    tatlCb.setTimeInterval(ti);
                    tatlCb.setId(refCb.getId() + TATL);
                    tatlCb.setOriginalId(refCb.getId());
                    setTemporaryLimit(tatlCb);
                    criticalBranches.add(tatlCb);

                    CriticalBranchType patlCb = (CriticalBranchType) refCb.clone();
                    patlCb.setTimeInterval(ti);
                    patlCb.setId(refCb.getId() + PATL);
                    patlCb.setOriginalId(refCb.getId());
                    patlCb.setComplexVariantId(statesWithCrac.get(cracCreationContext.getCrac().getState(refCb.getOutage().getId(), cracCreationContext.getCrac().getLastInstant())));
                    setPermanentLimit(patlCb);
                    criticalBranches.add(patlCb);
                }
            }
        });

        return criticalBranches;
    }

    private List<IndependantComplexVariant> getComplexVariantsOfSuccesfulInterval(FbConstraintCreationContext cracCreationContext, RaoResult raoResult, Map<State, String> statesWithCra) {

        List<IndependantComplexVariant> complexVariants = new ArrayList<>();

        TimeIntervalType ti = new TimeIntervalType();
        ti.setV(IntervalUtil.getCurrentTimeInterval(OffsetDateTime.ofInstant(interval.getStart(), ZoneOffset.UTC)));

        Map<String, IndependantComplexVariant> nativeVariants = getComplexVariantsForInstant(cracCreationContext.getTimeStamp(), flowBasedConstraintDocument);
        statesWithCra.forEach((state, variantId) -> {

            Set<NetworkAction> activatedNa = raoResult.getActivatedNetworkActionsDuringState(state);
            Set<RangeAction<?>> activatedRa = raoResult.getActivatedRangeActionsDuringState(state);

            IndependantComplexVariant complexVariant = new IndependantComplexVariant();
            complexVariant.setId(variantId);
            complexVariant.setName(getMergedName(activatedNa, activatedRa));
            complexVariant.setTimeInterval(ti);
            complexVariant.setTsoOrigin(getTsoOrigin(activatedNa, activatedRa));

            activatedNa.forEach(na -> updateComplexVariantWithNetworkAction(complexVariant, nativeVariants.get(na.getId()), state.getContingency().orElseThrow().getId()));
            activatedRa.forEach(ra -> updateComplexVariantWithPstAction(complexVariant, nativeVariants.get(ra.getId()), state.getContingency().orElseThrow().getId(), raoResult.getOptimizedTapOnState(state, (PstRangeAction) ra)));

            complexVariants.add(complexVariant);
        });

        return complexVariants;
    }

    private Network getNetworkOfTaskDto(ProcessFileDto cgmProcessFile) {
        try (InputStream networkInputStream = minioAdapter.getFileFromFullPath(cgmProcessFile.getFilePath())) {
            return Network.read(cgmProcessFile.getFilename(), networkInputStream);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import network of task %s", taskDto.getTimestamp()), e);
        }
    }

    private RaoResult getRaoResultOfTaskDto(Crac crac, ProcessFileDto raoResultProcessFile) {
        try (InputStream raoResultInputStream = minioAdapter.getFileFromFullPath(raoResultProcessFile.getFilePath())) {
            return RaoResult.read(raoResultInputStream, crac);
        } catch (IOException e) {
            throw new CoreCCPostProcessingInternalException(String.format("Cannot import RAO result of hourly RAO response of instant %s", taskDto.getTimestamp()), e);
        }
    }

    private static void setPermanentLimit(CriticalBranchType criticalBranch) {
        if (criticalBranch.getPermanentImaxA() != null) {
            criticalBranch.setImaxA(criticalBranch.getPermanentImaxA());
            criticalBranch.setImaxFactor(null);
        } else if (criticalBranch.getPermanentImaxFactor() != null) {
            criticalBranch.setImaxA(null);
            criticalBranch.setImaxFactor(criticalBranch.getPermanentImaxFactor());
        }
        criticalBranch.setPermanentImaxA(null);
        criticalBranch.setTemporaryImaxA(null);
        criticalBranch.setPermanentImaxFactor(null);
        criticalBranch.setTemporaryImaxFactor(null);
    }

    private static void setTemporaryLimit(CriticalBranchType criticalBranch) {
        if (criticalBranch.getTemporaryImaxA() != null) {
            criticalBranch.setImaxA(criticalBranch.getTemporaryImaxA());
            criticalBranch.setImaxFactor(null);
        } else if (criticalBranch.getTemporaryImaxFactor() != null) {
            criticalBranch.setImaxA(null);
            criticalBranch.setImaxFactor(criticalBranch.getTemporaryImaxFactor());
        }
        criticalBranch.setPermanentImaxA(null);
        criticalBranch.setTemporaryImaxA(null);
        criticalBranch.setPermanentImaxFactor(null);
        criticalBranch.setTemporaryImaxFactor(null);
    }

    private static String getMergedName(Set<NetworkAction> activatedNa, Set<RangeAction<?>> activatedRa) {

        String naNames = activatedNa.stream()
                .map(RemedialAction::getName)
                .collect(Collectors.joining(";"));

        String raNames = activatedRa.stream()
                .map(RemedialAction::getName)
                .collect(Collectors.joining(";"));

        if (activatedNa.isEmpty()) {
            return raNames;
        } else if (activatedRa.isEmpty()) {
            return naNames;
        } else {
            return naNames + ";" + raNames;
        }
    }

    private String getTsoOrigin(Set<NetworkAction> activatedNa, Set<RangeAction<?>> activatedRa) {

        Set<String> raTsos = activatedNa.stream()
                .map(RemedialAction::getOperator)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        raTsos.addAll(activatedRa.stream()
                .map(RemedialAction::getOperator)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        if (raTsos.size() == 1) {
            return raTsos.iterator().next();
        } else {
            return "XX";
        }
    }

    private static void updateComplexVariantWithNetworkAction(IndependantComplexVariant complexVariantToUpdate, IndependantComplexVariant refComplexVariant, String contingencyId) {

        IndependantComplexVariant temporaryClone = (IndependantComplexVariant) refComplexVariant.clone();
        temporaryClone.getActionsSet().get(0).getAfterCOList().getAfterCOId().clear();
        temporaryClone.getActionsSet().get(0).getAfterCOList().getAfterCOId().add(contingencyId);
        complexVariantToUpdate.getActionsSet().add(temporaryClone.getActionsSet().get(0));
    }

    private static void updateComplexVariantWithPstAction(IndependantComplexVariant complexVariantToUpdate, IndependantComplexVariant refComplexVariant, String contingencyId, int tapPosition) {

        IndependantComplexVariant temporaryClone = (IndependantComplexVariant) refComplexVariant.clone();

        temporaryClone.getActionsSet().get(0).getAfterCOList().getAfterCOId().clear();
        temporaryClone.getActionsSet().get(0).getAfterCOList().getAfterCOId().add(contingencyId);

        ObjectFactory objectFactory = new ObjectFactory();
        // remove range and relative range
        List<ActionType> actions = temporaryClone.getActionsSet().get(0).getAction(); //only one actions set
        ActionType newAction = objectFactory.createActionType();
        newAction.setType(actions.get(0).getType()); //only one action
        newAction.getContent().add(actions.get(0).getContent().get(0)); //white space
        newAction.getContent().add(actions.get(0).getContent().get(1)); //only keep branch
        newAction.getContent().add(actions.get(0).getContent().get(actions.get(0).getContent().size() - 1)); //last white space
        actions.clear();
        actions.add(newAction);

        // update complex variant value with tapPosition value
        List<Serializable> contents = temporaryClone.getActionsSet().get(0).getAction().get(0).getContent();
        //indentation is not correct when <value> is added, need to remove contents.last and add contents.first to have the correct indentation
        Serializable lastLine = contents.get(contents.size() - 1);
        contents.remove(contents.size() - 1); //remove last line
        contents.add(contents.get(0)); //white space line
        contents.add(objectFactory.createActionTypeValue(String.valueOf(tapPosition))); //value content
        contents.add(lastLine); //last white space

        complexVariantToUpdate.getActionsSet().add(temporaryClone.getActionsSet().get(0));
    }

    private static Map<State, String> getUIDOfStatesWithCra(CracCreationContext cracCreationContext, RaoResult raoResult, String instant) {

        int hour = OffsetDateTime.parse(instant).getHour();
        int uniqueIdIterator = 1;
        Map<State, String> stateMap = new HashMap<>();

        for (State state : cracCreationContext.getCrac().getStates(cracCreationContext.getCrac().getLastInstant())) {
            if (!raoResult.getActivatedNetworkActionsDuringState(state).isEmpty() || !raoResult.getActivatedRangeActionsDuringState(state).isEmpty()) {
                stateMap.put(state, String.format("CRA_%02d%04d", hour, uniqueIdIterator));
                uniqueIdIterator++;
            }
        }
        return stateMap;
    }

    private static Map<String, CriticalBranchType> getCriticalBranchesForInstant(OffsetDateTime offsetDateTime, FlowBasedConstraintDocument flowBasedConstraintDocument) {

        Map<String, CriticalBranchType> nativeCbs = new HashMap<>();

        for (CriticalBranchType cb : flowBasedConstraintDocument.getCriticalBranches().getCriticalBranch()) {
            if (!nativeCbs.containsKey(cb.getId()) || IntervalUtil.isInTimeInterval(offsetDateTime, cb.getTimeInterval().getV())) {
                nativeCbs.put(cb.getId(), cb);
            }
        }
        return nativeCbs;
    }

    private static Map<String, IndependantComplexVariant> getComplexVariantsForInstant(OffsetDateTime offsetDateTime, FlowBasedConstraintDocument flowBasedConstraintDocument) {

        Map<String, IndependantComplexVariant> nativeVariants = new HashMap<>();

        for (IndependantComplexVariant cv : flowBasedConstraintDocument.getComplexVariants().getComplexVariant()) {
            if (!nativeVariants.containsKey(cv.getId()) || IntervalUtil.isInTimeInterval(offsetDateTime, cv.getTimeInterval().getV())) {
                nativeVariants.put(cv.getId(), cv);
            }
        }
        return nativeVariants;
    }
}
