package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.ContactPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPPHOTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPPHOTransformer.class);


    public static void transform(String version,
                                 PPPHO parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatientPhone(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPPHO parser) {
        return null;
    }

    public static void createPatientPhone(PPPHO parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          BartsCsvHelper csvHelper,
                                          String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        //if no number, then nothing to process
        CsvCell numberCell = parser.getPhoneNumber();
        if (numberCell.isEmpty()) {
            return;
        }

        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        if (patientBuilder == null) {
            LOG.warn("Skipping PPPHO record for " + milleniumPersonIdCell.getString() + " as no MRN->Person mapping found");
            return;
        }

        //we always fully recreate the phone record on the patient so just remove any matching one already there
        CsvCell phoneIdCell = parser.getMillenniumPhoneId();
        ContactPointBuilder.removeExistingAddress(patientBuilder, phoneIdCell.getString());

        //if the record is inactive, we've already removed the phone from the patient so jsut return out
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        String number = numberCell.getString();

        CsvCell extensionCell = parser.getExtension();
        if (!extensionCell.isEmpty()) {
            number += " " + extensionCell.getString();
        }

        ContactPoint.ContactPointUse use = null;
        ContactPoint.ContactPointSystem system = null;

        CsvCell phoneTypeCell = parser.getPhoneTypeCode();
        if (!phoneTypeCell.isEmpty() && phoneTypeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                                                                                CernerCodeValueRef.PHONE_TYPE,
                                                                                phoneTypeCell.getLong());

            use = convertPhoneType(cernerCodeValueRef.getCodeMeaningTxt());
            system = convertPhoneSystem(cernerCodeValueRef.getCodeMeaningTxt());
        }

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
        contactPointBuilder.setId(phoneIdCell.getString(), phoneIdCell);
        contactPointBuilder.setUse(use, phoneTypeCell);
        contactPointBuilder.setSystem(system, phoneTypeCell);
        contactPointBuilder.setValue(number, numberCell, extensionCell);

        CsvCell startDate = parser.getBeginEffectiveDateTime();
        if (!startDate.isEmpty()) {
            contactPointBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDateTime();
        if (!endDate.isEmpty()) {
            contactPointBuilder.setEndDate(endDate.getDate(), endDate);
        }
    }


    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType) {

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        switch (phoneType) {
            case "HOME":
            case "VHOME":
            case "PHOME":
            case "USUAL":
            case "FAX PERS":
            case "PAGER PERS":
                return ContactPoint.ContactPointUse.HOME;
            case "FAX BUS":
            case "PROFESSIONAL":
            case "SECBUSINESS":
                return ContactPoint.ContactPointUse.WORK;
            case "MOBILE":
                return ContactPoint.ContactPointUse.MOBILE;
            case "FAX PREV":
            case "PREVIOUS":
            case "PAGER PREV":
                return ContactPoint.ContactPointUse.OLD;
            case "FAX TEMP":
                return ContactPoint.ContactPointUse.TEMP;
            default:
                return ContactPoint.ContactPointUse.TEMP;
        }
    }

    private static ContactPoint.ContactPointSystem convertPhoneSystem(String phoneType) {

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        switch (phoneType) {
            case "ALTERNATE":
            case "BILLING":
            case "EMC":
            case "PHONEEPRESCR":
            case "HOME":
            case "MOBILE":
            case "OS AFTERHOUR":
            case "OS PHONE":
            case "OS BK OFFICE":
            case "PREVIOUS":
            case "PHOME":
            case "PROFESSIONAL":
            case "CARETEAM":
            case "SECBUSINESS":
            case "TECHNICAL":
            case "USUAL":
            case "VHOME":
            case "VERIFY":
                return ContactPoint.ContactPointSystem.PHONE;
            case "FAX BUS":
            case "FAXEPRESCR":
            case "FAX ALT":
            case "FAX BILL":
            case "FAX PERS":
            case "FAX PREV":
            case "OS FAX":
                return ContactPoint.ContactPointSystem.FAX;
            case "PAGER ALT":
            case "PAGER PREV":
            case "PAGER PERS":
            case "OS PAGER":
            case "PAGER BILL":
            case "PAGING":
                return ContactPoint.ContactPointSystem.PAGER;
            default:
                return ContactPoint.ContactPointSystem.OTHER;
        }
    }

}

