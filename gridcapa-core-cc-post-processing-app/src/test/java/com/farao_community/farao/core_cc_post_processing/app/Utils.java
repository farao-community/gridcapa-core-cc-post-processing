/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app;

import com.farao_community.farao.gridcapa.task_manager.api.*;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author Thomas Bouquet {@literal <thomas.bouquet at rte-france.com>}
 */
public class Utils {

    private static final Map<TaskStatus, UUID> STATUS_TO_UUID = Map.of(TaskStatus.SUCCESS, UUID.fromString("4fb56583-bcec-4ed9-9839-0984b7324989"), TaskStatus.CREATED, UUID.fromString("2fcbb4fa-8b26-415a-a252-027746f387f4"), TaskStatus.RUNNING, UUID.fromString("b4efda15-92c5-431b-a17f-9c5f6d8a6437"), TaskStatus.ERROR, UUID.fromString("6e3e0ef2-96e4-4649-82d4-374f103038d4"), TaskStatus.NOT_CREATED, UUID.fromString("4a77f922-703f-4061-9288-83f6814543c5"), TaskStatus.READY, UUID.fromString("cfd187bf-22b3-4e1f-ad5b-6bf041c13780"), TaskStatus.PENDING, UUID.fromString("67fd6a16-b67c-4374-953e-e1f5288c1cb2"), TaskStatus.INTERRUPTED, UUID.fromString("24c780de-5839-4bc5-9eb9-7f4aff6b5522"), TaskStatus.STOPPING, UUID.fromString("00087855-69ff-49ee-9304-5bcf19adf3ca"));
    private static final OffsetDateTime TIMESTAMP = OffsetDateTime.parse("2023-08-21T15:16:45Z");
    private static final ProcessFileDto CRAC_PROCESS_FILE = new ProcessFileDto("/CORE/CC/crac.xml", "CBCORA", ProcessFileStatus.VALIDATED, "crac.xml", TIMESTAMP);
    private static final ProcessFileDto CNE_FILE_DTO = new ProcessFileDto("/CORE/CC/cne.xml", "CNE", ProcessFileStatus.VALIDATED, "cne.xml", TIMESTAMP);
    private static final ProcessFileDto CGM_FILE_DTO = new ProcessFileDto("/CORE/CC/network.uct", "CGM_OUT", ProcessFileStatus.VALIDATED, "network.uct", TIMESTAMP);
    private static final ProcessFileDto METADATA_FILE_DTO = new ProcessFileDto("/CORE/CC/metadata.csv", "METADATA", ProcessFileStatus.VALIDATED, "metadata.csv", TIMESTAMP);
    private static final ProcessFileDto RAO_RESULT_FILE_DTO = new ProcessFileDto("/CORE/CC/raoResult.json", "RAO_RESULT", ProcessFileStatus.VALIDATED, "raoResult.json", TIMESTAMP);
    private static final List<ProcessFileDto> INPUTS = List.of(CRAC_PROCESS_FILE);
    private static final List<ProcessFileDto> OUTPUTS = List.of(CNE_FILE_DTO, CGM_FILE_DTO, METADATA_FILE_DTO, RAO_RESULT_FILE_DTO);
    private static final List<ProcessEventDto> PROCESS_EVENTS = List.of();

    public static TaskDto makeTask(TaskStatus status) {
        UUID uuid = STATUS_TO_UUID.get(status);
        return new TaskDto(uuid, TIMESTAMP, status, INPUTS, OUTPUTS, PROCESS_EVENTS);
    }
}
