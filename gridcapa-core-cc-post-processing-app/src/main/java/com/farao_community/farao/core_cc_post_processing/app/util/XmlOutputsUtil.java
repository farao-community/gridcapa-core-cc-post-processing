
package com.farao_community.farao.core_cc_post_processing.app.util;

import com.farao_community.farao.core_cc_post_processing.app.exception.CoreCCPostProcessingInternalException;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.time.OffsetDateTime;

/**
 * @author Mohamed BenRejeb {@literal <mohamed.ben-rejeb at rte-france.com>}
 */
public final class XmlOutputsUtil {

    private XmlOutputsUtil() {
        throw new AssertionError("Utility class should not be constructed");
    }

    public static XMLGregorianCalendar getXMLGregorianCurrentTime() {
        XMLGregorianCalendar xmlGregorianCalendar;
        try {
            xmlGregorianCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(OffsetDateTime.now().toString());
            xmlGregorianCalendar.setMillisecond(DatatypeConstants.FIELD_UNDEFINED);
            xmlGregorianCalendar.setTimezone(0);
        } catch (DatatypeConfigurationException e) {
            throw new CoreCCPostProcessingInternalException("Impossible to create XmlGregorianCalendar current time", e);
        }
        return xmlGregorianCalendar;
    }
}
