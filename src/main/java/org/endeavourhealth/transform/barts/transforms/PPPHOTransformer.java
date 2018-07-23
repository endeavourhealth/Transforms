package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPPHOTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPPHOTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientPhone((PPPHO) parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void createPatientPhone(PPPHO parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell phoneIdCell = parser.getMillenniumPhoneId();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPPHOPreTransformer.PPPHO_ID_TO_PERSON_ID, phoneIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                if (patientBuilder != null) {
                    ContactPointBuilder.removeExistingContactPoint(patientBuilder, phoneIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        //if no number, then nothing to process
        CsvCell numberCell = parser.getPhoneNumber();
        if (numberCell.isEmpty()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        String number = numberCell.getString();

        //just append the extension on to the number
        CsvCell extensionCell = parser.getExtension();
        if (!extensionCell.isEmpty()) {
            number += " " + extensionCell.getString();
        }

        //we always fully recreate the phone record on the patient so just remove any matching one already there
        ContactPointBuilder.removeExistingContactPoint(patientBuilder, phoneIdCell.getString());

        //and remove any instance of this phone number created by the ADT feed
        removeExistingContactPointWithoutIdByValue(patientBuilder, number);

        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
        contactPointBuilder.setId(phoneIdCell.getString(), phoneIdCell);
        contactPointBuilder.setValue(number, numberCell, extensionCell);

        CsvCell phoneTypeCell = parser.getPhoneTypeCode();
        String phoneTypeDesc = null;
        if (!BartsCsvHelper.isEmptyOrIsZero(phoneTypeCell)) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.PHONE_TYPE, phoneTypeCell);
            phoneTypeDesc = codeRef.getCodeMeaningTxt();
            ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc);
            contactPointBuilder.setUse(use, phoneTypeCell);
        }

        CsvCell phoneMethodCell = parser.getContactMethodCode();
        if (!phoneMethodCell.isEmpty() && phoneMethodCell.getLong() > 0) {

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.PHONE_METHOD, phoneMethodCell);
            String phoneMethodDesc = codeRef.getCodeMeaningTxt();

            ContactPoint.ContactPointSystem system = convertPhoneSystem(phoneTypeDesc, phoneMethodDesc);
            contactPointBuilder.setSystem(system, phoneTypeCell, phoneMethodCell);
        }

        CsvCell startDate = parser.getBeginEffectiveDateTime();
        if (!startDate.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(startDate);
            contactPointBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDateTime();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = BartsCsvHelper.parseDate(endDate);
            contactPointBuilder.setEndDate(d, endDate);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }

    private static void removeExistingContactPointWithoutIdByValue(PatientBuilder patientBuilder, String number) {
        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasTelecom()) {
            return;
        }

        List<ContactPoint> telecoms = patient.getTelecom();
        for (int i=telecoms.size()-1; i>=0; i--) {
            ContactPoint telecom = telecoms.get(i);

            //if this one has an ID it was created by this data warehouse feed, so don't try to remove it
            if (telecom.hasId()) {
                continue;
            }

            if (telecom.hasValue()
                    && !telecom.getValue().equalsIgnoreCase(number)) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            telecoms.remove(i);
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
            case "BUSINESS":
                return ContactPoint.ContactPointUse.WORK;

            case "MOBILE":
                return ContactPoint.ContactPointUse.MOBILE;

            case "FAX PREV":
            case "PREVIOUS":
            case "PAGER PREV":
                return ContactPoint.ContactPointUse.OLD;

            case "FAX TEMP":
            case "TEMPORARY":
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

        return ContactPoint.ContactPointSystem.OTHER;
    }
}

