package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class MaternityBirth extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(MaternityBirth.class);

    public MaternityBirth(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                "yyyy-MM-dd", //different date format to standard
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "BABY_PERSON_ID",
                "MRN",
                "NHS",
                "BABY_NAME",
                "PREGNANCY_ID",
                "PREGNANCY_CHILD_ID",
                "DELETE_IND",
                "BABY_ALIVE_LABOUR_ONSET_NM_ID",
                "BABY_ALIVE_LABOUR_ONSET_DESC",
                "BIRTH_ODR_NBR",
                "BIRTH_NBR",
                "BIRTH_LOC_NM_ID",
                "BIRTH_LOC_DESC",
                "BIRTH_LOC_DETAIL_NM_ID",
                "BIRTH_LOC_DETAIL_DESC",
                "MEM_RUPTURE_DT_TM",
                "BIRTH_DT_TM",
                "GEST_DEL",
                "DEL_OUTCOME_CD",
                "DEL_OUTCOME_DESC",
                "NEO_OUTCOME_CD",
                "NEO_OUTCOME_DESC",
                "PREG_OUTCOME_CD",
                "PREG_OUTCOME_DESC",
                "PRES_DEL_NM_ID",
                "PRES_DEL_DESC",
                "DEL_METHOD_CD",
                "DEL_METHOD_DESC",
                "GRADE_OF_URGENCY_OF_THE_CAESAREAN_NM_ID",
                "GRADE_OF_URGENCY_OF_THE_CAESAREAN_DESC",
                "BIRTH_WT",
                "CORD_PH_RESULT",
                "WATERBIRTH_NM_ID",
                "WATERBIRTH_DESC",
                "APGAR_1MIN",
                "APGAR_5MIN",
                "RESP_5MIN_NM_ID",
                "RESP_5MIN_DESC",
                "RESP_10MIN_NM_ID",
                "RESP_10MIN_DESC",
                "RESP_20MIN_NM_ID",
                "RESP_20MIN_DESC",
                "HR_5MIN_NM_ID",
                "HR_5MIN_DESC",
                "HR_10MIN_NM_ID",
                "HR_10MIN_DESC",
                "HR_20MIN_NM_ID",
                "HR_20MIN_DESC",
                "FEEDING_METHOD_NM_ID",
                "FEEDING_METHOD_DESC",
                "BBA_NM_ID",
                "BBA_DESC",
                "DELIVERED_BY",
                "DEL_PRSNL_STATUS_NM_ID",
                "DEL_PRSNL_STATUS_DESC",
                "NOTIFYING_MW",
                "OTHERS_ATT_DEL_NM_ID",
                "OTHERS_ATT_DEL_DESC",
                "LAB_START_DT_TM",
                "LENGTH_LABOUR_TOT",
                "C/SECT_INDICATION_NM_ID",
                "C/SECT_INDICATION_DESC",
                "NEO_CARE_LEVEL_NM_ID",
                "NEO_CARE_LEVEL_DESC",
                "NB_GEST_ASSESS_NM_ID",
                "NB_GEST_ASSESS_DESC",
                "RESUS_METHOD_NM_ID",
                "RESUS_METHOD_DESC",
                "NB_SEX_CD",
                "NB_SEX_DESC",
                "MOTHER_COMPLICATION_SCT_CD",
                "MOTHER_COMPLICATION_DESC",
                "FETAL_COMPLICATION_SCT_CD",
                "FETAL_COMPLICATION_DESC",
                "NEONATAL_COMPLICATION_SCT_CD",
                "NEONATAL_COMPLICATION_DESC",
                "NEO_DEATH_DT_TM",
                "PREGNANCY_CHILD_SEQ_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}

/*
public class Birth extends AbstractFixedParser {

    public Birth(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }
    public String getDOB() {
        return super.getString("DOB");
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

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("DOB",    321, 8));

        return ret;
    }


}*/
