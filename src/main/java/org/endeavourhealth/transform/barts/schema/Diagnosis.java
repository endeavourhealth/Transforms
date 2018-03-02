package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Diagnosis extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Diagnosis.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public Diagnosis(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
    }

    public Long getDiagnosisId() {
        return Long.parseLong(super.getString("DiagnosisId").trim().split("\\.")[0]);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getActiveIndicator() {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getPersonId() {
        return Long.parseLong(super.getString("PersonId").trim().split("\\.")[0]);
    }

    public Long getEncounterId() {
        return Long.parseLong(super.getString("EncounterId").trim().split("\\.")[0]);
    }
    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getFINNbr() {
        return super.getString("FINNbr").trim();
    }
    public String getDiagnosis() {
        return super.getString("Diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("DiagnosisDate");
    }
    public String getDiagnosisDateAsString() throws TransformException {
        return super.getString("DiagnosisDate");
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

    @Override
    protected String getFileTypeDescription() {
        return "CDS Diagnosis file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return true;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("DiagnosisId",             1, 14));
        ret.add(new FixedParserField("Update_DT_TM",          16, 20));
        ret.add(new FixedParserField("ActiveIndicator",          37, 11));
        ret.add(new FixedParserField("PersonId",    49, 14));
        ret.add(new FixedParserField("EncounterId",    64, 14));
        ret.add(new FixedParserField("MRN",    79, 20));
        ret.add(new FixedParserField("FINNbr",    100, 20));
        ret.add(new FixedParserField("Diagnosis",    121, 150));
        ret.add(new FixedParserField("DiagnosisDate",    314, 11));
        ret.add(new FixedParserField("DiagnosisCode",    650, 20));
        ret.add(new FixedParserField("Vocabulary",    671, 20));
        ret.add(new FixedParserField("SecondaryDescription",    713, 500));
        
        return ret;
    }

}