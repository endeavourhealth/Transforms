package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SusOutpatientTail extends AbstractFixedParser implements CdsTailRecordI {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientTail.class);

    public SusOutpatientTail(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CDS_DATE_FORMAT, BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }


    public CsvCell getCdsUniqueId() {
        return super.getCell("CDS_Unique_Identifier");
    }

    public CsvCell getFinNumber() {
        return super.getCell("Fin_Nbr");
    }

    public CsvCell getEncounterId() {
        CsvCell ret =  super.getCell("Encounter_ID");
        if (ret.getString().contains(".")) {
            int i= new Double(ret.getString()).intValue();
            ret=ret.factoryWithNewValue(ret,Integer.toString(i));
        }
        return ret;
     }

    public CsvCell getEpisodeId() {
        return super.getCell("Episode_ID");
    }

    public CsvCell getPersonId() {
        return super.getCell("Person_ID");
    }

    public CsvCell getResponsiblePersonnelId() {
        return super.getCell("Responsible_HCP_Personal_ID");
    }

    public CsvCell getCdsActivityDate() { return  super.getCell("CDS_Activity_Date");}
    public CsvCell getCdsUpdateType() { return super.getCell("CDS_Update_Type");}
    public CsvCell getLocalPatientId() { return super.getCell("Local_Patient_ID");}
    public CsvCell getNhsNumber() { return super.getCell("NHS_Number");}
    public CsvCell getTreatmentFunctionCd() { return super.getCell("TREATMENT_FUNCTION_CD");}


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
        ret.add(new FixedParserField("Referral_To_Treatment_Period_Start_Date", 133, 8));
        ret.add(new FixedParserField("CDS_Activity_Date", 141, 8));
        ret.add(new FixedParserField("CDS_Activity_Time", 149, 6));
        ret.add(new FixedParserField("CDS_Deceased_Date", 155, 8));
        ret.add(new FixedParserField("CDS_Deceased_Time", 163, 6));
        ret.add(new FixedParserField("Referral_To_Treatment_Period_Status", 169, 2));
        ret.add(new FixedParserField("Outcome_of_Attendance", 171, 2));
        ret.add(new FixedParserField("CDS_Update_Type", 173, 1));
        ret.add(new FixedParserField("CDS_Applicable_Date", 174, 8));
        ret.add(new FixedParserField("CDS_Applicable_Time", 182, 6));
        ret.add(new FixedParserField("CDS_Extract_Date", 188, 8));
        ret.add(new FixedParserField("CDS_Extract_Time", 196, 6));
        ret.add(new FixedParserField("ADC_Comments", 202, 255));
        ret.add(new FixedParserField("Referral_To_Treatment_Code_Value", 457, 10));
        ret.add(new FixedParserField("Parent_Entity_Name", 467, 20));
        ret.add(new FixedParserField("Parent_Entity_ID", 487, 10));
        ret.add(new FixedParserField("GP_Personal_ID", 497, 10));
        ret.add(new FixedParserField("Responsible_HCP_Personal_ID", 507, 10));
        ret.add(new FixedParserField("TREATMENT_FUNCTION_CD", 517, 10));
        ret.add(new FixedParserField("MAIN_SPECIALTY_CD", 527, 10));
        ret.add(new FixedParserField("CDS_Record_Updated_Date", 537, 8));
        ret.add(new FixedParserField("CDS_Record_Updated_Time", 545, 6));

        return ret;
    }
}
