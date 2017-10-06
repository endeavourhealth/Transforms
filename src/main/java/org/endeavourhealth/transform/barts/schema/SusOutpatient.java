package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class SusOutpatient extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatient.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusOutpatient(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("CDSVersion",             1, 6));
        addFieldList(new FixedParserField("CDSRecordType",          7, 3));
        addFieldList(new FixedParserField("CDSReplacementgroup",    10, 3));
        addFieldList(new FixedParserField("CDSUniqueID",    16, 35));
        addFieldList(new FixedParserField("CDSUpdateType",    51, 1));

        addFieldList(new FixedParserField("MRN",    284, 10));
        addFieldList(new FixedParserField("DOB",    321, 8));
        addFieldList(new FixedParserField("PatientTitle",    471, 35));
        addFieldList(new FixedParserField("PatientForename",    506, 35));
        addFieldList(new FixedParserField("PatientSurname",    541, 35));

        addFieldList(new FixedParserField("AddressType",    646, 2));
        addFieldList(new FixedParserField("UnstructuredAddress",    648, 175));
        addFieldList(new FixedParserField("Address1",    823, 35));
        addFieldList(new FixedParserField("Address2",    858, 35));
        addFieldList(new FixedParserField("Address3",    893, 35));
        addFieldList(new FixedParserField("Address4",    928, 35));
        addFieldList(new FixedParserField("Address5",    963, 35));
        addFieldList(new FixedParserField("PostCode",    998, 8));

        addFieldList(new FixedParserField("ConsultantCode",    1023, 8));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1047, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1054, 350));

        addFieldList(new FixedParserField("AppointmentDate",    1681, 8));
        addFieldList(new FixedParserField("AppointmentTime",    1689, 6));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    1798, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    1802, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    1838, 2000));

    }

    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }

    public Date getAppointmentDate() throws TransformException {
        return super.getDate("AppointmentDate");
    }
    public Date getAppointmentTime() throws TransformException {
        return super.getTime("AppointmentTime");
    }
    public Date getAppointmentDateTime() throws TransformException {
        return super.getDateTime("AppointmentDate", "AppointmentTime");
    }

}