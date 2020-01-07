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

public class CriticalCare extends AbstractFixedParser implements CdsRecordCriticalCareI {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergency.class);

    public CriticalCare(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CDS_DATE_FORMAT, BartsCsvToFhirTransformer.CDS_TIME_FORMAT);
    }

    public CsvCell getLocalPatientId() { return super.getCell("LocalPatientID");}
    public CsvCell getNhsNumber() { return super.getCell("NHSNumber");}

    public CsvCell getSpellNumber() { return super.getCell("HospitalProviderSpellNumber");}
    public CsvCell getEpisodeNumber() { return super.getCell("EpisodeNumber");}
    public CsvCell getCdsUniqueId() { return super.getCell("CDSUniqueIdentifier");}
    public CsvCell getCriticalCareTypeID() { return super.getCell("CriticalCareTypeID");}

    public CsvCell getCriticalCareIdentifier() { return super.getCell("CriticalCareLocalIdentifier");}
    public CsvCell getCriticalCareStartDate() { return super.getCell("CriticalCareStartDate");}
    public CsvCell getCriticalCareStartTime() { return super.getCell("CriticalCareStartTime");}
    public CsvCell getCriticalCareUnitFunction() { return super.getCell("CriticalCareUnitFunction");}
    public CsvCell getCriticalCareAdmissionSource() { return super.getCell("CriticalCareAdmissionSource");}
    public CsvCell getCriticalCareSourceLocation() { return super.getCell("CriticalCareSourceLocation");}
    public CsvCell getCriticalCareAdmissionType() { return super.getCell("CriticalCareAdmissionType");}
    public CsvCell getGestationLengthAtDelivery() { return super.getCell("GestationLengthAtDelivery");}
    public CsvCell getAdvancedRespiratorySupportDays() { return super.getCell("AdvancedRespiratorySupportDays");}
    public CsvCell getBasicRespiratorySupportsDays() { return super.getCell("BasicRespiratorySupportsDays");}
    public CsvCell getAdvancedCardiovascularSupportDays() { return super.getCell("AdvancedCardiovascularSupportDays");}
    public CsvCell getBasicCardiovascularSupportDays() { return super.getCell("BasicCardiovascularSupportDays");}
    public CsvCell getRenalSupportDays() { return super.getCell("RenalSupportDays");}
    public CsvCell getNeurologicalSupportDays() { return super.getCell("NeurologicalSupportDays");}
    public CsvCell getGastroIntestinalSupportDays() { return super.getCell("GastroIntestinalSupportDays");}
    public CsvCell getDermatologicalSupportDays() { return super.getCell("DermatologicalSupportDays");}
    public CsvCell getLiverSupportDays() { return super.getCell("LiverSupportDays");}
    public CsvCell getOrganSupportMaximum() { return super.getCell("OrganSupportMaximum");}
    public CsvCell getCriticalCareLevel2Days() { return super.getCell("CriticalCareLevel2Days");}
    public CsvCell getCriticalCareLevel3Days() { return super.getCell("CriticalCareLevel3Days");}
    public CsvCell getCriticalCareDischargeDate() { return super.getCell("CriticalCareDischargeDate");}
    public CsvCell getCriticalCareDischargeTime() { return super.getCell("CriticalCareDischargeTime");}
    public CsvCell getCriticalCareDischargeReadyDate() { return super.getCell("CriticalCareDischargeReadyDate");}
    public CsvCell getCriticalCareDischargeReadyTime() { return super.getCell("CriticalCareDischargeReadyTime");}
    public CsvCell getCriticalCareDischargeStatus() { return super.getCell("CriticalCareDischargeStatus");}
    public CsvCell getCriticalCareDischargeDestination() { return super.getCell("CriticalCareDischargeDestination");}
    public CsvCell getCriticalCareDischargeLocation() { return super.getCell("CriticalCareDischargeLocation");}

    public CsvCell getCareActivity1() { return super.getCell("CareActivity1");}
    public CsvCell getCareActivity2100() { return super.getCell("CareActivity2100");}


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

        //file definition in Indigo 4 Standard BT Translation Service v6-2 Specification - v1.0.1.xls

//CRITICAL CARE DETAILS
        ret.add(new FixedParserField("LocalPatientID", 1, 10));
        ret.add(new FixedParserField("NHSNumber", 11, 10));
        ret.add(new FixedParserField("HospitalProviderSpellNumber", 21, 12));
        ret.add(new FixedParserField("EpisodeNumber", 33, 2));
        ret.add(new FixedParserField("CDSUniqueIdentifier", 35, 35));
        ret.add(new FixedParserField("CriticalCareTypeID", 70, 2));
//ADMISSION CHARACTERISTICS
        ret.add(new FixedParserField("CriticalCareLocalIdentifier", 72, 8));
        ret.add(new FixedParserField("CriticalCareStartDate", 80, 8));
        ret.add(new FixedParserField("CriticalCareStartTime", 88, 6));
        ret.add(new FixedParserField("CriticalCareUnitFunction", 94, 2));
        ret.add(new FixedParserField("GestationLengthAtDelivery", 96, 2));
        ret.add(new FixedParserField("UnitBedConfiguration", 98, 2));
        ret.add(new FixedParserField("CriticalCareAdmissionSource", 100, 2));
        ret.add(new FixedParserField("CriticalCareSourceLocation", 102, 2));
        ret.add(new FixedParserField("CriticalCareAdmissionType", 104, 2));
//CARE ACTIVITY GROUP
        ret.add(new FixedParserField("AdvancedRespiratorySupportDays", 106, 3));
        ret.add(new FixedParserField("BasicRespiratorySupportsDays", 109, 3));
        ret.add(new FixedParserField("AdvancedCardiovascularSupportDays", 112, 3));
        ret.add(new FixedParserField("BasicCardiovascularSupportDays", 115, 3));
        ret.add(new FixedParserField("RenalSupportDays", 118, 3));
        ret.add(new FixedParserField("NeurologicalSupportDays", 121, 3));
        ret.add(new FixedParserField("GastroIntestinalSupportDays", 124, 3));
        ret.add(new FixedParserField("DermatologicalSupportDays", 127, 3));
        ret.add(new FixedParserField("LiverSupportDays", 130, 3));
        ret.add(new FixedParserField("OrganSupportMaximum", 133, 2));
        ret.add(new FixedParserField("CriticalCareLevel2Days", 135, 3));
        ret.add(new FixedParserField("CriticalCareLevel3Days", 138, 3));
//DISCHARGE CHARACTERISTICS
        ret.add(new FixedParserField("CriticalCareDischargeDate", 141, 8));
        ret.add(new FixedParserField("CriticalCareDischargeTime", 149, 6));
        ret.add(new FixedParserField("CriticalCareDischargeReadyDate", 155, 8));
        ret.add(new FixedParserField("CriticalCareDischargeReadyTime", 163, 6));
        ret.add(new FixedParserField("CriticalCareDischargeStatus", 169, 2));
        ret.add(new FixedParserField("CriticalCareDischargeDestination", 171, 2));
        ret.add(new FixedParserField("CriticalCareDischargeLocation", 173, 2));
//CARE ACTIVITY GROUP & CODES
        ret.add(new FixedParserField("CareActivity1", 175, 135));
        ret.add(new FixedParserField("ActivityDateCriticalCare", 175, 8));
        ret.add(new FixedParserField("PersonWeight", 183, 7));
        ret.add(new FixedParserField("CriticalCareActivityCode#1", 190, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#2", 192, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#3", 194, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#4", 196, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#5", 198, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#6", 200, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#7", 202, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#8", 204, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#9", 206, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#10", 208, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#11", 210, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#12", 212, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#13", 214, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#14", 216, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#15", 218, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#16", 220, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#17", 222, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#18", 224, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#19", 226, 2));
        ret.add(new FixedParserField("CriticalCareActivityCode#20", 228, 2));
//HIGH COST DRUGS OPCS
        ret.add(new FixedParserField("HighCostDrugsOPCS#1", 230, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#2", 234, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#3", 238, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#4", 242, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#5", 246, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#6", 250, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#7", 254, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#8", 258, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#9", 262, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#10", 266, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#11", 270, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#12", 274, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#13", 278, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#14", 282, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#15", 286, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#16", 290, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#17", 294, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#18", 298, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#19", 302, 4));
        ret.add(new FixedParserField("HighCostDrugsOPCS#20", 306, 4));
//CARE ACTIVITY GROUP & CODES
        ret.add(new FixedParserField("CareActivity2100", 310, 13365));

        return ret;
    }
}
