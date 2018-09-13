package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusOutpatient extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatient.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusOutpatient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
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

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return false;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("CDSUniqueID",    16, 35));
        ret.add(new FixedParserField("CDSUpdateType",    51, 1));

        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("NHSNo",    308, 10));
        ret.add(new FixedParserField("DOB",    321, 8));
        ret.add(new FixedParserField("PatientTitle",    471, 35));
        ret.add(new FixedParserField("PatientForename",    506, 35));
        ret.add(new FixedParserField("PatientSurname",    541, 35));

        ret.add(new FixedParserField("AddressType",    646, 2));
        ret.add(new FixedParserField("UnstructuredAddress",    648, 175));
        ret.add(new FixedParserField("Address1",    823, 35));
        ret.add(new FixedParserField("Address2",    858, 35));
        ret.add(new FixedParserField("Address3",    893, 35));
        ret.add(new FixedParserField("Address4",    928, 35));
        ret.add(new FixedParserField("Address5",    963, 35));
        ret.add(new FixedParserField("PostCode",    998, 8));

        ret.add(new FixedParserField("Gender",    1018, 1));
        ret.add(new FixedParserField("EthnicCategory",    1021, 2));

        ret.add(new FixedParserField("ConsultantCode",    1023, 8));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1047, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1054, 350));

        // 1	Discharged from CONSULTANT's care (last attendance), 2	Another APPOINTMENT given, 3	APPOINTMENT to be made at a later date
        ret.add(new FixedParserField("OutcomeCode",    1680, 1));
        ret.add(new FixedParserField("AppointmentDate",    1681, 8));
        ret.add(new FixedParserField("AppointmentTime",    1689, 6));
        ret.add(new FixedParserField("ExpectedDurationMinutes",    1695, 3));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    1798, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    1802, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    1838, 2000));


        ret.add(new FixedParserField("GP",    4532, 8));
        ret.add(new FixedParserField("GPPractice",    4540, 12));
        
        return ret;
    }
}