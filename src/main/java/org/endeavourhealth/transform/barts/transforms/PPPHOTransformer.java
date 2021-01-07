package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.*;
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
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            //There are a small number of cases where all the fields are empty (including person ID) but in all examined
            //cases, we've never previously received a valid record, so can just ignore them
            if (!personIdCell.isEmpty()) {
                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
                if (patientBuilder != null) {
                    //LOG.trace("Deleting phone " + phoneIdCell.getString() + " which maps to person " + personIdStr + " with FHIR patient\r" + FhirSerializationHelper.serializeResource(patientBuilder.getResource()));

                    ContactPointBuilder.removeExistingContactPointById(patientBuilder, phoneIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
                }
            }
            return;
        }

        //if no number, then nothing to process - seems like in some cases the Millennium user doesn't delete
        //a phone number, but just amends it to have a blank number
        //Do this BEFORE we get the patient builder out, otherwise it never gets returned
        CsvCell numberCell = parser.getPhoneNumber();
        if (numberCell.isEmpty()) {
            //SD-296 - removing warning as this is legitimate and not need to track it happening
            //TransformWarnings.log(LOG, csvHelper, "Ignoring PPPHO record {} for person ID {} because number is empty", phoneIdCell, personIdCell);
            return;
        }

        //get our patient resource builder
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            return;
        }

        try {

            //by removing from the patient and re-adding, we're constantly changing the order of the
            //contact points, so attempt to reuse the existing one for the ID
            ContactPointBuilder contactPointBuilder = ContactPointBuilder.findOrCreateForId(patientBuilder, phoneIdCell);
            contactPointBuilder.reset(); //clear down as we've got a full phone record to replace all content with

            //we always fully recreate the phone record on the patient so just remove any matching one already there
            /*ContactPointBuilder.removeExistingContactPointById(patientBuilder, phoneIdCell.getString());

            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setId(phoneIdCell.getString(), phoneIdCell);*/

            String number = numberCell.getString();

            //just append the extension on to the number
            CsvCell extensionCell = parser.getExtension();
            if (!extensionCell.isEmpty()) {
                number += " " + extensionCell.getString();
            }

            contactPointBuilder.setValue(number, numberCell, extensionCell);

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

            boolean isActive = true;
            if (contactPointBuilder.getContactPoint().hasPeriod()) {
                isActive = PeriodHelper.isActive(contactPointBuilder.getContactPoint().getPeriod());
            }

            CsvCell phoneTypeCell = parser.getPhoneTypeCode();
            CsvCell phoneTypeDescCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_TYPE, phoneTypeCell);
            String phoneTypeDesc = phoneTypeDescCell.getString();
            ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc, isActive);
            contactPointBuilder.setUse(use, phoneTypeCell, phoneTypeDescCell);

            CsvCell phoneMethodCell = parser.getContactMethodCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(phoneMethodCell)) {

                CsvCell phoneMethodDescCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_METHOD, phoneMethodCell);
                String phoneMethodDesc = phoneMethodDescCell.getString();

                ContactPoint.ContactPointSystem system = convertPhoneSystem(phoneTypeDesc, phoneMethodDesc);
                contactPointBuilder.setSystem(system, phoneTypeCell, phoneMethodCell, phoneMethodDescCell);

            } else {
                //the phone method is zero for some records, but we still need to look up a system
                ContactPoint.ContactPointSystem system = convertPhoneSystem(phoneTypeDesc, null);
                contactPointBuilder.setSystem(system, phoneTypeCell);
            }

            //and remove any instance of this phone number created by the ADT feed
            ContactPoint newOne = contactPointBuilder.getContactPoint();
            removeExistingContactPointWithoutIdByValue(patientBuilder, newOne);


        } finally {
            //no need to save the resource now, as all patient resources are saved at the end of the PP... files
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        }
    }

    public static void removeExistingContactPointWithoutIdByValue(PatientBuilder patientBuilder, ContactPoint newOne) {

        Patient patient = (Patient) patientBuilder.getResource();
        if (!patient.hasTelecom()) {
            return;
        }

        List<ContactPoint> telecoms = patient.getTelecom();
        List<ContactPoint> duplicates = ContactPointHelper.findMatches(newOne, telecoms);

        for (ContactPoint duplicate: duplicates) {

            //if this name has an ID it was created by this data warehouse feed, so don't try to remove it
            if (duplicate.hasId()) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            telecoms.remove(duplicate);
        }
    }

    /*public static void removeExistingContactPointWithoutIdByValue(PatientBuilder patientBuilder, String number) {
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
    }*/


    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType, boolean isActive) throws Exception {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return ContactPoint.ContactPointUse.OLD;
        }

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

        //there are only four distinct methods in the code ref table
        //but there are cases where we don't have a method, so need to handle null
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
                default:
                    return ContactPoint.ContactPointSystem.OTHER;
            }

        } else {
            //in the cases where we don't get a method, all the data received looked like phone numbers
            return ContactPoint.ContactPointSystem.PHONE;
        }
    }
}

