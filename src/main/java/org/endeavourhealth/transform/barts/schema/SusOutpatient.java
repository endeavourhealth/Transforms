package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SusOutpatient extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatient.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusOutpatient(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("CDSVersion",             1, 6));
        addFieldList(new FixedParserField("CDSRecordType",          7, 3));
        addFieldList(new FixedParserField("CDSReplacementgroup",    10, 3));
        addFieldList(new FixedParserField("CDSUniqueID",    16, 35));
        addFieldList(new FixedParserField("CDSUpdateType",    51, 1));

        addFieldList(new FixedParserField("MRN",    284, 10));
        addFieldList(new FixedParserField("NHSNo",    308, 10));
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

        addFieldList(new FixedParserField("Gender",    1018, 1));
        addFieldList(new FixedParserField("EthnicCategory",    1021, 2));

        addFieldList(new FixedParserField("ConsultantCode",    1023, 8));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1047, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1054, 350));

        // 1	Discharged from CONSULTANT's care (last attendance), 2	Another APPOINTMENT given, 3	APPOINTMENT to be made at a later date
        addFieldList(new FixedParserField("OutcomeCode",    1680, 1));
        addFieldList(new FixedParserField("AppointmentDate",    1681, 8));
        addFieldList(new FixedParserField("AppointmentTime",    1689, 6));
        addFieldList(new FixedParserField("ExpectedDurationMinutes",    1695, 3));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    1798, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    1802, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    1838, 2000));


        addFieldList(new FixedParserField("GP",    4532, 8));
        addFieldList(new FixedParserField("GPPractice",    4540, 12));

    }


    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }

    public String getOutcomeCode() {
        return super.getString("OutcomeCode");
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
    public int getExpectedDurationMinutes() throws TransformException {
        return super.getInt("ExpectedDurationMinutes");
    }
    public Date getExpectedLeavingDateTime() throws TransformException {
        return new Date(getAppointmentDateTime().getTime() + (getExpectedDurationMinutes() * 60000));
    }

}