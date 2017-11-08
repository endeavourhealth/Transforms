package org.endeavourhealth.transform.common;

import org.endeavourhealth.transform.emis.openhr.schema.VocDatePart;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Date;

public class XmlDateHelper {

    public static Date convertDate(XMLGregorianCalendar xmlDate) {
        return xmlDate.toGregorianCalendar().getTime();
    }
}
