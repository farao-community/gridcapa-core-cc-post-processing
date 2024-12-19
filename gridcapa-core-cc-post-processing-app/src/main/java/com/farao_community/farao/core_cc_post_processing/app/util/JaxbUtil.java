/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import com.farao_community.farao.core_cc_post_processing.app.outputs.rao_response.ResponseMessageType;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

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

    public static byte[] marshallMessageAndSetJaxbProperties(ResponseMessageType responseMessage) {
        try {
            StringWriter stringWriter = new StringWriter();
            JAXBContext jaxbContext = JAXBContext.newInstance(ResponseMessageType.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            String eventMessage = "EventMessage";
            QName qName = new QName(XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI, eventMessage);
            JAXBElement<ResponseMessageType> root = new JAXBElement<>(qName, ResponseMessageType.class, responseMessage);
            jaxbMarshaller.marshal(root, stringWriter);
            return stringWriter.toString()
                .replace("xsi:EventMessage", "EventMessage")
                .replace("<EventMessage", "<EventMessage xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"")
                .replace("<ResponseItems", "<ResponseItems xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://unicorn.com/Response/response-payload\"")
                .getBytes();
        } catch (Exception e) {
            throw new CoreCCPostProcessingInternalException("Exception occurred during RAO Response export.", e);
        }
    }
}
