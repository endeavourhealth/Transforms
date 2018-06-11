package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.ContactPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPPHOTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPPHOTransformer.class);


    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createPatientPhone((PPPHO) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
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
            TransformWarnings.log(LOG, parser, "Skipping PPPHO record for {} as no MRN->Person mapping found", milleniumPersonIdCell);
            return;
        }

        //we always fully recreate the phone record on the patient so just remove any matching one already there
        CsvCell phoneIdCell = parser.getMillenniumPhoneId();
        ContactPointBuilder.removeExistingAddress(patientBuilder, phoneIdCell.getString());

        //if the record is inactive, we've already removed the phone from the patient so just return out
        //and the patient builder will be saved at the end of the transform
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        String number = numberCell.getString();

        //just append the extension on to the number
        CsvCell extensionCell = parser.getExtension();
        if (!extensionCell.isEmpty()) {
            number += " " + extensionCell.getString();
        }


        CsvCell phoneTypeCell = parser.getPhoneTypeCode();
        String phoneTypeDesc = null;
        if (!phoneTypeCell.isEmpty() && phoneTypeCell.getLong() > 0) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.PHONE_TYPE, phoneTypeCell);
            phoneTypeDesc = codeRef.getCodeMeaningTxt();

        }

        CsvCell phoneMethodCell = parser.getContactMethodCode();
        String phoneMethodDesc = null;
        if (!phoneMethodCell.isEmpty() && phoneMethodCell.getLong() > 0) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.PHONE_METHOD, phoneMethodCell);
            phoneMethodDesc = codeRef.getCodeMeaningTxt();
        }

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
        contactPointBuilder.setId(phoneIdCell.getString(), phoneIdCell);
        contactPointBuilder.setValue(number, numberCell, extensionCell);

        ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc);
        contactPointBuilder.setUse(use, phoneTypeCell);

        ContactPoint.ContactPointSystem system = convertPhoneSystem(phoneTypeDesc, phoneMethodDesc);
        contactPointBuilder.setSystem(system, phoneTypeCell, phoneMethodCell);

        CsvCell startDate = parser.getBeginEffectiveDateTime();
        if (!startDate.isEmpty()) {
            contactPointBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDateTime();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            contactPointBuilder.setEndDate(endDate.getDate(), endDate);
        }
    }


    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType) throws Exception {

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        //this is based on the full list of types from CODE_REF where the set is 43
        switch (phoneType) {
            case "HOME":
            case "VHOME":
            case "PHOME":
            case "USUAL":
            case "PAGER PERS":
            case "FAX PERS":
            case "VERIFY":
                return ContactPoint.ContactPointUse.HOME;

            case "FAX BUS":
            case "PROFESSIONAL":
            case "SECBUSINESS":
            case "CARETEAM":
            case "PHONEEPRESCR":
            case "AAM": //Automated Answering Machine
            case "BILLING":
            case "PAGER ALT":
            case "PAGING":
            case "PAGER BILL":
            case "FAX BILL":
            case "FAX ALT":
            case "ALTERNATE":
            case "EXTSECEMAIL":
            case "INTSECEMAIL":
            case "FAXEPRESCR":
            case "EMC":  //Emergency Phone
            case "TECHNICAL":
            case "OS AFTERHOUR":
            case "OS PHONE":
            case "OS PAGER":
            case "OS BK OFFICE":
            case "OS FAX":
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
                throw new TransformException("Unsupported phone type [" + phoneType + "]");
        }
    }

    private static ContactPoint.ContactPointSystem convertPhoneSystem(String type, String method) throws Exception {

        //the method AND type both convey information about the type of contact point, so refer to both fields

        //the type tells us which are fax and pager contacts (see the above fn for the full list of known types)
        if (type != null) {
            switch (type) {
                case "FAX BUS":
                case "FAXEPRESCR":
                case "FAX ALT":
                case "FAX BILL":
                case "FAX PERS":
                case "FAX PREV":
                case "OS FAX":
                case "FAX TEMP":
                    return ContactPoint.ContactPointSystem.FAX;

                case "PAGER ALT":
                case "PAGER PREV":
                case "PAGER PERS":
                case "OS PAGER":
                case "PAGER BILL":
                case "PAGING":
                    return ContactPoint.ContactPointSystem.PAGER;
            }
        }

        //there are only four distinct methods in the code ref table
        if (method != null) {
            switch (method) {
                case "TEL":
                    return ContactPoint.ContactPointSystem.PHONE;
                case "FAX":
                    return ContactPoint.ContactPointSystem.FAX;
                case "MAILTO":
                    return ContactPoint.ContactPointSystem.EMAIL;
                case "TEXTPHONE":
                    return ContactPoint.ContactPointSystem.OTHER;
            }
        }

        //we should never get here, unless new types and methods are added to Cerner
        throw new TransformException("Unsupported phone type [" + type + "] and method [" + method + "]");
    }
}

