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
    public CsvCell getMrn() {
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

    public CsvCell getStatusDate() {
        return super.getCell("StatusDate");
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
        ret.add(new FixedParserField("StatusDate",    612, 11));
        ret.add(new FixedParserField("StatusLifecycle",    624, 20));
        ret.add(new FixedParserField("Severity",    690, 50));
        ret.add(new FixedParserField("ProblemCode",    1025, 20));
        ret.add(new FixedParserField("Vocabulary",    1046, 20));
        ret.add(new FixedParserField("Description",    1088, 500));
        ret.add(new FixedParserField("UpdatedBy",    1589, 50));
        
        return ret;
        
    }

}