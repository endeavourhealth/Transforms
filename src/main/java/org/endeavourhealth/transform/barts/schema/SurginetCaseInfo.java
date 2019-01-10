package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SurginetCaseInfo extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(SurginetCaseInfo.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";

    public SurginetCaseInfo(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
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

        //definition in Surginet_Case_Info_Extracts_ForDiscovery_v18.xlsx

        ret.add(new FixedParserField("SURG_CASE_ID", 1, 15));
        ret.add(new FixedParserField("SCHEDULE_ID", 16, 15));
        ret.add(new FixedParserField("UPDT_DT_TM", 31, 21));
        ret.add(new FixedParserField("ACTIVE_IND", 52, 11));
        ret.add(new FixedParserField("CASE_NBR", 63, 101));
        ret.add(new FixedParserField("CASE_NBR_LOCN", 164, 41));
        ret.add(new FixedParserField("CASE_NBR_YR", 205, 12));
        ret.add(new FixedParserField("CASE_NBR_NO", 217, 12));
        ret.add(new FixedParserField("PATIENT_NAME", 229, 101));
        ret.add(new FixedParserField("MRN", 330, 101));
        ret.add(new FixedParserField("FIN", 431, 101));
        ret.add(new FixedParserField("INSTITUTION", 532, 41));
        ret.add(new FixedParserField("DEPARTMENT", 573, 41));
        ret.add(new FixedParserField("SURGICAL_AREA", 614, 41));
        ret.add(new FixedParserField("SCH_SURGICAL_AREA", 655, 41));
        ret.add(new FixedParserField("SCH_OPERATING_ROOM", 696, 41));
        ret.add(new FixedParserField("SCH_SURGERY_DATE", 737, 17));
        ret.add(new FixedParserField("SCH_SURGERY_TIME", 754, 17));
        ret.add(new FixedParserField("SCH_DUR", 771, 12));
        ret.add(new FixedParserField("INTENDED_PRIMARY_PROC", 783, 61));
        ret.add(new FixedParserField("MODIFIER", 844, 101));
        ret.add(new FixedParserField("NCEPOD", 945, 256));
        ret.add(new FixedParserField("ASA_CLASS", 1201, 41));
        ret.add(new FixedParserField("CASE_LEVEL", 1242, 41));
        ret.add(new FixedParserField("WOUND_CLASS", 1283, 41));
        ret.add(new FixedParserField("SURG_SPECIALTY", 1324, 41));
        ret.add(new FixedParserField("SCHED_PAT_TYPE", 1365, 41));
        ret.add(new FixedParserField("SCHED_TYPE", 1406, 41));
        ret.add(new FixedParserField("PRIMARY_SURGEON", 1447, 101));
        ret.add(new FixedParserField("PRIM_SURG_CONS_CODE", 1548, 101));
        ret.add(new FixedParserField("ANESTHETIST", 1649, 101));
        ret.add(new FixedParserField("SCHEDULE_SEQ", 1750, 13));
        ret.add(new FixedParserField("APPT_STATUS", 1763, 41));
        ret.add(new FixedParserField("CHECKIN_DTTM", 1804, 18));
        ret.add(new FixedParserField("CHECKIN_BY", 1822, 101));
        ret.add(new FixedParserField("SURG_COMPLETE_IND", 1923, 18));
        ret.add(new FixedParserField("CANCEL_DTTM", 1941, 21));
        ret.add(new FixedParserField("CANCEL_REASON", 1962, 41));
        ret.add(new FixedParserField("SURG_CANCEL_REASON", 2003, 256));
        ret.add(new FixedParserField("CANCEL_REQ_BY", 2259, 101));
        ret.add(new FixedParserField("CANCEL_REQ_BY_TEXT", 2360, 101));
        ret.add(new FixedParserField("RESCHED_DTTM", 2461, 21));
        ret.add(new FixedParserField("RESCHED_REASON", 2482, 41));
        ret.add(new FixedParserField("SURG_CANCEL_REASON_RESCH", 2523, 256));
        ret.add(new FixedParserField("TEMPLATE_ID", 2779, 15));
        ret.add(new FixedParserField("TEMPLATE_BEG_DTTM", 2794, 21));
        ret.add(new FixedParserField("TEMPLATE_END_DTTM", 2815, 21));
        ret.add(new FixedParserField("TEMPLATE", 2836, 101));
        ret.add(new FixedParserField("SESSION_ID", 2937, 15));
        ret.add(new FixedParserField("SESSION", 2952, 101));
        ret.add(new FixedParserField("SESSION_BEG_DTTM", 3053, 21));
        ret.add(new FixedParserField("SESSION_END_DTTM", 3074, 21));
        ret.add(new FixedParserField("PERSON_ID", 3095, 15));
        ret.add(new FixedParserField("ENCNTR_ID", 3110, 15));
        ret.add(new FixedParserField("SCH_EVENT_ID", 3125, 15));
        ret.add(new FixedParserField("PM_WAIT_LIST_ID", 3140, 16));
        ret.add(new FixedParserField("UPDATED_BY", 3156, 101));

        return ret;
    }
}
