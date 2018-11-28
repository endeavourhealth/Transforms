package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SusEmergencyCareDataSetTail extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyCareDataSetTail.class);

    public SusEmergencyCareDataSetTail(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CDS_DATE_FORMAT, BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
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

        //note that this file has the same spec as the old AEA tails file (i.e. there's no separate spec for this)
        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDS_Unique_Identifier", 1, 35));
        ret.add(new FixedParserField("CDS_Record_Type", 36, 3));
        ret.add(new FixedParserField("Patient_Pathway_Identifier", 39, 20));
        ret.add(new FixedParserField("Organisation_Code_Patient_Pathway", 59, 12));
        ret.add(new FixedParserField("Local_Patient_ID", 71, 10));
        ret.add(new FixedParserField("NHS_Number", 81, 10));
        ret.add(new FixedParserField("Fin_Nbr", 91, 12));
        ret.add(new FixedParserField("Encounter_ID", 103, 10));
        ret.add(new FixedParserField("Person_ID", 113, 10));
        ret.add(new FixedParserField("Episode_ID", 123, 10));
        ret.add(new FixedParserField("CDS_Activity_Date", 133, 8));
        ret.add(new FixedParserField("CDS_Activity_Time", 141, 6));
        ret.add(new FixedParserField("CDS_Deceased_Date", 147, 8));
        ret.add(new FixedParserField("CDS_Deceased_Time", 155, 6));
        ret.add(new FixedParserField("CDS_Update_Type", 161, 1));
        ret.add(new FixedParserField("CDS_Applicable_Date", 162, 8));
        ret.add(new FixedParserField("CDS_Applicable_Time", 170, 6));
        ret.add(new FixedParserField("CDS_Extract_Date", 176, 8));
        ret.add(new FixedParserField("CDS_Extract_Time", 184, 6));
        ret.add(new FixedParserField("Referral_To_Treatment_Code_Value", 190, 10));
        ret.add(new FixedParserField("Parent_Entity_Name", 200, 20));
        ret.add(new FixedParserField("Parent_Entity_ID", 220, 10));
        ret.add(new FixedParserField("GP_Personal_ID", 230, 10));
        ret.add(new FixedParserField("Responsible_HCP_Personal_ID", 240, 10));
        ret.add(new FixedParserField("TREATMENT_FUNCTION_CD", 250, 10));
        ret.add(new FixedParserField("MAIN_SPECIALTY_CD", 260, 10));
        ret.add(new FixedParserField("CDS_Record_Updated_Date", 270, 8));
        ret.add(new FixedParserField("CDS_Record_Updated_Time", 278, 6));

        return ret;
    }
}