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
        return Long.parseLong(super.getString("diagnosis_id").trim().split("\\.")[0]);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("updt_dt_tm");
    }

    public String getActiveIndicator() {
        return super.getString("active_ind");
    }

    public boolean isActive() {
        int val = super.getInt("active_ind");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getPersonId() {
        return Long.parseLong(super.getString("person_id").trim().split("\\.")[0]);
    }

    public Long getEncounterId() {
        return Long.parseLong(super.getString("encntr_id").trim().split("\\.")[0]);
    }

    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }

    public String getFINNbr() {
        return super.getString("fin").trim();
    }

    public String getDiagnosis() {
        return super.getString("diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("diag_dt");
    }

    public String getDiagnosisDateAsString() throws TransformException {
        return super.getString("diag_dt");
    }

    public String getDiagnosisCode() {
        return super.getString("diag_code").trim();
    }

    public String getVocabulary() {
        return super.getString("vocab").trim();
    }

    public String getSecondaryDescription() {
        return super.getString("secondary_descriptions").trim();
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

        ret.add(new FixedParserField("diagnosis_id", 1, 14));
        ret.add(new FixedParserField("updt_dttm", 16, 20));
        ret.add(new FixedParserField("active_ind", 37, 11));
        ret.add(new FixedParserField("person_id", 49, 14));
        ret.add(new FixedParserField("encntr_id", 64, 14));
        ret.add(new FixedParserField("MRN", 79, 20));
        ret.add(new FixedParserField("fin", 100, 20));
        ret.add(new FixedParserField("diagnosis", 121, 150));
        ret.add(new FixedParserField("qualifier", 272, 20));
        ret.add(new FixedParserField("confirmation", 293, 20));
        ret.add(new FixedParserField("diag_dt", 314, 11));
        ret.add(new FixedParserField("classification", 326, 20));
        ret.add(new FixedParserField("clin_service", 347, 30));
        ret.add(new FixedParserField("diag_type", 378, 20));
        ret.add(new FixedParserField("rank", 399, 20));
        ret.add(new FixedParserField("diag_prnsl", 420, 50));
        ret.add(new FixedParserField("severity_class", 471, 20));
        ret.add(new FixedParserField("severity", 492, 20));
        ret.add(new FixedParserField("certainty", 513, 20));
        ret.add(new FixedParserField("probability", 534, 14));
        ret.add(new FixedParserField("org_name", 549, 100));
        ret.add(new FixedParserField("diag_code", 650, 20));
        ret.add(new FixedParserField("vocab", 671, 20));
        ret.add(new FixedParserField("axis", 692, 20));
        ret.add(new FixedParserField("secondary_descriptions", 713, 500));

        return ret;
    }

    /*@Override
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
    }*/

}