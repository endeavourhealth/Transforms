package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Problem extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Problem.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";

    public Problem(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
    }

    public CsvCell getProblemId() {
        return super.getCell("problem_id");
    }

    public CsvCell getPersonId() {
        return super.getCell("person_id");
    }

    /**
     * person_id is suffixed with ".00" so we have this version to give us a nice version to work with
     */
    public CsvCell getPersonIdSanitised() {
        CsvCell ret = getPersonId();
        if (ret.getString().contains(".")) {
            int i = new Double(ret.getString()).intValue();
            ret = CsvCell.factoryWithNewValue(ret, Integer.toString(i));
        }
        return ret;
    }

    public CsvCell getUpdateDateTime() {
        return super.getCell("update_dt_tm");
    }

    public CsvCell getMrn() {
        return super.getCell("mrn");
    }

    public CsvCell getProblem() {
        return super.getCell("problem");
    }

    public CsvCell getAnnotatedDisp() {
        return super.getCell("annotated_disp");
    }

    public CsvCell getConfirmation() {
        return super.getCell("confirmation");
    }

    public CsvCell getOnsetDate() {
        return super.getCell("onset_date");
    }

    public CsvCell getStatusDate() {
        return super.getCell("status_date");
    }

    public CsvCell getStatusLifecycle() {
        return super.getCell("status_lifecycle");
    }

    public CsvCell getQualifier() {
        return super.getCell("qualifier");
    }

    public CsvCell getSeverity() {
        return super.getCell("severity");
    }

    public CsvCell getPersistence() {
        return super.getCell("persistence");
    }

    public CsvCell getPrognosis() {
        return super.getCell("prognosis");
    }

    public CsvCell getClassification() {
        return super.getCell("classification");
    }

    public CsvCell getProblemCode() {
        return super.getCell("problem_code");
    }

    public CsvCell getVocabulary() {
        return super.getCell("vocab");
    }

    public CsvCell getDescription() {
        return super.getCell("secondary_descriptions");
    }

    public CsvCell getUpdatedBy() {
        return super.getCell("updated_by");
    }

    public CsvCell getRanking() {
        return super.getCell("ranking");
    }

    public CsvCell getOrgName() {
        return super.getCell("org_name");
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

        ret.add(new FixedParserField("problem_id", 1, 14));
        ret.add(new FixedParserField("update_dt_tm", 16, 20));
        ret.add(new FixedParserField("person_id", 37, 14));
        ret.add(new FixedParserField("mrn", 52, 40));
        ret.add(new FixedParserField("problem", 93, 200));
        ret.add(new FixedParserField("annotated_disp", 294, 200));
        ret.add(new FixedParserField("qualifier", 495, 20));
        ret.add(new FixedParserField("confirmation", 516, 20));
        ret.add(new FixedParserField("classification", 537, 20));
        ret.add(new FixedParserField("onset_precision", 558, 20));
        ret.add(new FixedParserField("onset_date", 579, 11));
        ret.add(new FixedParserField("status_precision", 591, 20));
        ret.add(new FixedParserField("status_date", 612, 11));
        ret.add(new FixedParserField("status_lifecycle", 624, 20));
        ret.add(new FixedParserField("lifecycle_cancelled_rsn", 645, 23));
        ret.add(new FixedParserField("severity_class", 669, 20));
        ret.add(new FixedParserField("severity", 690, 50));
        ret.add(new FixedParserField("course", 741, 20));
        ret.add(new FixedParserField("persistence", 762, 20));
        ret.add(new FixedParserField("ranking", 783, 20));
        ret.add(new FixedParserField("certainty", 804, 20));
        ret.add(new FixedParserField("probability", 825, 14));
        ret.add(new FixedParserField("prognosis", 840, 20));
        ret.add(new FixedParserField("person_aware", 861, 20));
        ret.add(new FixedParserField("family_aware", 882, 20));
        ret.add(new FixedParserField("prognosis_aware", 903, 20));
        ret.add(new FixedParserField("org_name", 924, 100));
        ret.add(new FixedParserField("problem_code", 1025, 20));
        ret.add(new FixedParserField("vocab", 1046, 20));
        ret.add(new FixedParserField("axis", 1067, 20));
        ret.add(new FixedParserField("secondary_descriptions", 1088, 500));
        ret.add(new FixedParserField("updated_by", 1589, 50));

        return ret;
    }

    /*@Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("ProblemId",             1, 11));
        ret.add(new FixedParserField("Update_DT_TM",          16, 20));
        ret.add(new FixedParserField("MRN",    52, 40));
        ret.add(new FixedParserField("Problem",    93, 200));
        ret.add(new FixedParserField("AnnotatedDisp",    294, 200));
        ret.add(new FixedParserField("Confirmation",    516, 20));
        ret.add(new FixedParserField("OnsetDate",    579, 11));
        ret.add(new FixedParserField("StatusDate",    612, 11));
        ret.add(new FixedParserField("StatusLifecycle",    624, 20));
        ret.add(new FixedParserField("Severity",    690, 50));
        ret.add(new FixedParserField("ProblemCode",    1025, 20));
        ret.add(new FixedParserField("Vocabulary",    1046, 20));
        ret.add(new FixedParserField("Description",    1088, 500));
        ret.add(new FixedParserField("UpdatedBy",    1589, 50));
        
        return ret;
        
    }*/

}