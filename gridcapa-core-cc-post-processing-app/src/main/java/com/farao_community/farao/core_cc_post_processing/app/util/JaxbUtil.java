/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.*;
import java.io.ByteArrayOutputStream;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 * @author Philippe Edwards {@literal <philippe.edwards at rte-france.com>}
 * @author Godelaine de Montmorillon {@literal <godelaine.demontmorillon at rte-france.com>}
 */
public final class JaxbUtil {

    private JaxbUtil() {
        throw new AssertionError("Utility class should not be constructed");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(JaxbUtil.class);

    public static <T> byte[] writeInBytes(Class<T> clazz, T type) {
        try {
            JAXBContext context = JAXBContext.newInstance(clazz);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            marshaller.marshal(type, bos);
            return bos.toByteArray();
        } catch (JAXBException e) {
            String errorMessage = String.format("Error occurred when writing content of object of type %s to bytes", clazz.getName());
            LOGGER.error(errorMessage);
            throw new CoreCCPostProcessingInternalException(errorMessage, e);
        }
    }
}
