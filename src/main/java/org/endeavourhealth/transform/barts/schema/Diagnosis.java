package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class Diagnosis extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Diagnosis.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public Diagnosis(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("DiagnosisId",             1, 14));
        addFieldList(new FixedParserField("Update_DT_TM",          16, 20));
        addFieldList(new FixedParserField("MRN",    79, 20));
        addFieldList(new FixedParserField("Diagnosis",    121, 150));
        addFieldList(new FixedParserField("DiagnosisDate",    314, 11));
        addFieldList(new FixedParserField("DiagnosisCode",    650, 20));
        addFieldList(new FixedParserField("Vocabulary",    671, 20));
        addFieldList(new FixedParserField("SecondaryDescription",    713, 500));

    }

    public Long getDiagnosisId() {
        String ret = super.getString("DiagnosisId").trim().split("\\.")[0];
        return Long.parseLong(ret);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getDiagnosis() {
        return super.getString("Diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("DiagnosisDate");
    }

    public String getDiagnosisCode() {
        return super.getString("DiagnosisCode").trim();
    }
    public String getVocabulary() {
        return super.getString("Vocabulary").trim();
    }
    public String getSecondaryDescription() {
        return super.getString("SecondaryDescription").trim();
    }

}