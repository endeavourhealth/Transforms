package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class SusEmergency extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergency.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusEmergency(String version, File f, boolean openParser) throws Exception {
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

        addFieldList(new FixedParserField("ArrivalDate",    1079, 8));
        addFieldList(new FixedParserField("ArrivalTime",    1087, 6));
        addFieldList(new FixedParserField("StaffCode",    1259, 3));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1264, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1271, 350));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    2496, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    2500, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    2536, 2000));

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


    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }



}