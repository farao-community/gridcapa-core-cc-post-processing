/*
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 */
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.*;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class JaxbUtil {

    private JaxbUtil() {
        throw new AssertionError("Utility class should not be constructed");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(JaxbUtil.class);

    public static <T> T unmarshalFile(Class<T> clazz, Path path) {
        try (InputStream fileContent = Files.newInputStream(path)) {
            JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            JAXBElement<T> requestMessageTypeElement = jaxbUnmarshaller.unmarshal(new StreamSource(fileContent), clazz);
            return requestMessageTypeElement.getValue();
        } catch (JAXBException | IOException e) {

            String errorMessage = String.format("Error occurred when converting xml file %s to object of type %s", path, clazz.getName());
            LOGGER.error(errorMessage);
            throw new CoreCCPostProcessingInternalException(errorMessage, e);
        }
    }

    public static <T> T unmarshalContent(Class<T> clazz, InputStream inputStream) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            JAXBElement<T> requestMessageTypeElement = jaxbUnmarshaller.unmarshal(new StreamSource(inputStream), clazz);
            return requestMessageTypeElement.getValue();
        } catch (JAXBException e) {
            String errorMessage = String.format("Error occurred when converting InputStream to object of type %s", clazz.getName());
            LOGGER.error(errorMessage);
            throw new CoreCCPostProcessingInternalException(errorMessage, e);
        }
    }

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
