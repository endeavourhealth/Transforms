package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    //all the below need re-implementing using CsvCells
    public CsvCell getDiagnosisId() {
        return super.getCell("diagnosis_id");
    }

    public CsvCell getUpdateDateTime() throws TransformException {
        return super.getCell("updt_dt_tm");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("active_ind");
    }

    public CsvCell isActive() {
        return super.getCell("active_ind");
    }

    public CsvCell getPersonId() {
        return super.getCell("person_id");
    }

    public CsvCell getEncounterId() {
        return super.getCell("encntr_id");
    }

    /**
     * encounter ID is suffixed with ".00" so we have this version to give us a nice version to work with
     */
    public CsvCell getEncounterIdSanitised() {
        CsvCell ret = getEncounterId();
        if (ret.getString().contains(".")) {
            int i = new Double(ret.getString()).intValue();
            ret = CsvCell.factoryWithNewValue(ret, Integer.toString(i));
        }
        return ret;
    }

    public CsvCell getMRN() {
        return super.getCell("MRN");
    }

    public CsvCell getFINNbr() {
        return super.getCell("fin");
    }

    public CsvCell getDiagnosis() {
        return super.getCell("diagnosis");
    }

    public CsvCell getQualifier() {
        return super.getCell("qualifier");
    }

    public CsvCell getClassification() {
        return super.getCell("classification");
    }
    public CsvCell getConfirmation() {
        return  super.getCell("confirmation");
    }

    public CsvCell getClinService() {
        return super.getCell("clin_service");
    }

    public CsvCell getDiagType() {
        return super.getCell("diag_type");
    }

    public CsvCell getRank() {
        return super.getCell("rank");
    }

    public CsvCell getSeverityClass() {
        return super.getCell("severity_class");
    }

    public CsvCell getSeverity() {
        return super.getCell("severity");
    }

    public CsvCell getCertainty() {
        return super.getCell("certainty");
    }

    public CsvCell getProbability() {
        return super.getCell("probability");
    }

    public CsvCell getOrgName() {
        return super.getCell("org_name");
    }

    public CsvCell getAxis() {
        return super.getCell("axis");
    }


    public CsvCell getDiagPrnsl() {
        return super.getCell("diag_prnsl");
    }

    public CsvCell getDiagnosisDate()  {
        return super.getCell("diag_dt");
    }

    public CsvCell getDiagnosisCode() {
        return super.getCell("diag_code");
    }

    public CsvCell getVocabulary() {
        return super.getCell("vocab");
    }

    public CsvCell getSecondaryDescription() {
        return super.getCell("secondary_descriptions");
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

}