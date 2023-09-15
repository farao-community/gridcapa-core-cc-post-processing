/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.services;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.util.IntervalUtil;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.FbConstraint;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.xsd.FlowBasedConstraintDocument;
import com.farao_community.farao.data.crac_creation.creator.fb_constraint.importer.FbConstraintImporter;
import com.farao_community.farao.gridcapa.task_manager.api.ProcessFileDto;
import com.farao_community.farao.gridcapa.task_manager.api.TaskDto;
import com.farao_community.farao.minio_adapter.starter.MinioAdapter;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import java.io.InputStream;
import java.util.*;

/**
 * @author Pengbo Wang {@literal <pengbo.wang at rte-international.com>}
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Baptiste Seguinot {@literal <baptiste.seguinot at rte-france.com}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
@Service
public class DailyF303Generator {

    private final MinioAdapter minioAdapter;

    public DailyF303Generator(MinioAdapter minioAdapter) {
        this.minioAdapter = minioAdapter;
    }

    public FlowBasedConstraintDocument generate(Map<TaskDto, ProcessFileDto> raoResults, Map<TaskDto, ProcessFileDto> cgms) {
        String cracFilePath = raoResults.keySet().stream()
            .findFirst().orElseThrow()
            .getInputs()
            .stream().filter(processFileDto -> processFileDto.getFileType().equals("CBCORA"))
            .findFirst().orElseThrow(() -> new CoreCCPostProcessingInternalException("task dto missing cbcora file"))
            .getFilePath();

        try (InputStream cracXmlInputStream = minioAdapter.getFile(cracFilePath.split("CORE/CC/")[1])) {
            // get native CRAC
            FbConstraint nativeCrac = new FbConstraintImporter().importNativeCrac(cracXmlInputStream);

            // generate F303Info for each hour of the initial CRAC
            Map<Integer, Interval> positionMap = IntervalUtil.getPositionsMap(nativeCrac.getDocument().getConstraintTimeInterval().getV());
            List<HourlyF303Info> hourlyF303Infos = new ArrayList<>();
            positionMap.values().forEach(interval -> {
                TaskDto taskDto =  getTaskDtoOfInterval(interval, raoResults.keySet());
                hourlyF303Infos.add(new HourlyF303InfoGenerator(nativeCrac, interval, taskDto, minioAdapter).generate(raoResults.get(taskDto), cgms.get(taskDto)));
            });

            // gather hourly info in one common document, cluster the elements that can be clusterized
            return new DailyF303Clusterizer(hourlyF303Infos, nativeCrac).generateClusterizedDocument();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during F303 file creation", e);
        }
    }

    private TaskDto getTaskDtoOfInterval(Interval interval, Set<TaskDto> taskDtos) {
        return taskDtos.stream().filter(taskDto -> interval.contains(taskDto.getTimestamp().toInstant())).findFirst()
            .orElseThrow(() -> new CoreCCPostProcessingInternalException(String.format("Cannot find taskDto for interval %s", interval)));
    }
}
