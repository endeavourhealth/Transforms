package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.barts.transforms.ProblemTransformer;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class Problem extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Problem.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public Problem(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("ProblemId",             1, 14));
        addFieldList(new FixedParserField("Update_DT_TM",          16, 20));
        addFieldList(new FixedParserField("MRN",    52, 40));
        addFieldList(new FixedParserField("Problem",    93, 200));
        addFieldList(new FixedParserField("AnnotatedDisp",    294, 200));
        addFieldList(new FixedParserField("OnsetDate",    579, 11));
        addFieldList(new FixedParserField("Severity",    690, 50));
        addFieldList(new FixedParserField("ProblemCode",    1025, 20));
        addFieldList(new FixedParserField("Vocabulary",    1046, 20));
        addFieldList(new FixedParserField("UpdatedBy",    1589, 50));

    }

    public Long getProblemId() {
        String ret = super.getString("ProblemId").split("\\.")[0];
        return Long.parseLong(ret);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }
    public String getproblem() {
        return super.getString("Problem");
    }
    public String getAnnotatedDisp() {
        return super.getString("AnnotatedDisp");
    }

    public Date getOnsetDate() throws TransformException {
        return super.getDate("OnsetDate");
    }

    public String getSeverity() {
        return super.getString("Severity");
    }
    public String getProblemCode() {
        return super.getString("ProblemCode");
    }
    public String getVocabulary() {
        return super.getString("Vocabulary");
    }
    public String getUpdatedBy() {
        return super.getString("UpdatedBy");
    }

}