package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusEmergency extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergency.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusEmergency(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
    }

    public Date getArrivalDate() throws TransformException {
        return super.getDate("ArrivalDate");
    }
    public Date getArrivalTime() throws TransformException {
        return super.getTime("ArrivalTime");
    }
    public Date getArrivalDateTime() throws TransformException {
        return super.getDateTime("ArrivalDate", "ArrivalTime");
    }

    public Date getDepartureDate() throws TransformException {
        return super.getDate("DepartureDate");
    }
    public Date getDepartureTime() throws TransformException {
        return super.getTime("DepartureTime");
    }
    public Date getDepartureDateTime() throws TransformException {
        return super.getDateTime("DepartureDate", "DepartureTime");
    }


    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }


    @Override
    protected String getFileTypeDescription() {
        return "SUS Emergency file";
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

        ret.add(new FixedParserField("GP",    1023, 8));
        ret.add(new FixedParserField("GPPractice",    1031, 12));

        ret.add(new FixedParserField("ArrivalDate",    1079, 8));
        ret.add(new FixedParserField("ArrivalTime",    1087, 6));
        ret.add(new FixedParserField("DepartureDate",    1139, 8));
        ret.add(new FixedParserField("DepartureTime",    1147, 6));

        ret.add(new FixedParserField("StaffCode",    1259, 3));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1264, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1271, 350));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    2496, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    2500, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    2536, 2000));

        return ret;
    }
}