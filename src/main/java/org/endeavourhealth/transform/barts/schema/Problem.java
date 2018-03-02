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
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public Problem(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
    }

    public CsvCell getProblemId() {
        return super.getCell("ProblemId");
    }

    public CsvCell getUpdateDateTime() {
        return super.getCell("Update_DT_TM");
    }
    public CsvCell getLocalPatientId() {
        return super.getCell("MRN");
    }
    public CsvCell getProblem() {
        return super.getCell("Problem");
    }
    public CsvCell getAnnotatedDisp() {
        return super.getCell("AnnotatedDisp");
    }
    public CsvCell getConfirmation() {
        return super.getCell("Confirmation");
    }

    public CsvCell getOnsetDate() {
        return super.getCell("OnsetDate");
    }

    public CsvCell getStatusLifecycle() {
        return super.getCell("StatusLifecycle");
    }
    public CsvCell getSeverity() {
        return super.getCell("Severity");
    }
    public CsvCell getProblemCode() {
        return super.getCell("ProblemCode");
    }
    public CsvCell getVocabulary() {
        return super.getCell("Vocabulary");
    }
    public CsvCell getDescription() {
        return super.getCell("Description");
    }
    public CsvCell getUpdatedBy() {
        return super.getCell("UpdatedBy");
    }
    
    /*public Long getProblemId() {
        String ret = super.getString("ProblemId").split("\\.")[0];
        return Long.parseLong(ret);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getProblem() {
        return super.getString("Problem").trim();
    }
    public String getAnnotatedDisp() {
        return super.getString("AnnotatedDisp").trim();
    }
    public String getConfirmation() {
        return super.getString("Confirmation");
    }

    public Date getOnsetDate() throws TransformException {
        return super.getDate("OnsetDate");
    }
    public String getOnsetDateAsString() throws TransformException {
        return super.getString("OnsetDate");
    }

    public String getStatusLifecycle() {
        return super.getString("StatusLifecycle");
    }
    public String getSeverity() {
        return super.getString("Severity").trim();
    }
    public String getProblemCode() {
        return super.getString("ProblemCode").trim();
    }
    public String getVocabulary() {
        return super.getString("Vocabulary").trim();
    }
    public String getDescription() {
        return super.getString("Description").trim();
    }
    public String getUpdatedBy() {
        return super.getString("UpdatedBy").trim();
    }*/

    @Override
    protected String getFileTypeDescription() {
        return "CDS Problem file";
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

        ret.add(new FixedParserField("ProblemId",             1, 11));
        ret.add(new FixedParserField("Update_DT_TM",          16, 20));
        ret.add(new FixedParserField("MRN",    52, 40));
        ret.add(new FixedParserField("Problem",    93, 200));
        ret.add(new FixedParserField("AnnotatedDisp",    294, 200));
        ret.add(new FixedParserField("Confirmation",    516, 20));
        ret.add(new FixedParserField("OnsetDate",    579, 11));
        ret.add(new FixedParserField("StatusLifecycle",    624, 20));
        ret.add(new FixedParserField("Severity",    690, 50));
        ret.add(new FixedParserField("ProblemCode",    1025, 20));
        ret.add(new FixedParserField("Vocabulary",    1046, 20));
        ret.add(new FixedParserField("Description",    1088, 500));
        ret.add(new FixedParserField("UpdatedBy",    1589, 50));
        
        return ret;
        
    }

}