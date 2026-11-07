package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCodeableConceptHelper;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPPHO;
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
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientPhone((PPPHO) parser, fhirResourceFiler, csvHelper,sourceSystem);
            }
        }
    }


    public static void createPatientPhone(PPPHO parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        CsvCell phoneIdCell = parser.getMillenniumPhoneId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            //There are a small number of cases where all the fields are empty (including person ID) but in all examined
            //cases, we've never previously received a valid record, so can just ignore them
            if (!personIdCell.isEmpty()) {
                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
                if (patientBuilder != null) {
                    //LOG.trace("Deleting phone " + phoneIdCell.getString() + " which maps to person " + personIdStr + " with FHIR patient\r" + FhirSerializationHelper.serializeResource(patientBuilder.getResource()));

                    ContactPointBuilder.removeExistingContactPointById(patientBuilder, phoneIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
                }
            }
            TransformWarnings.log(LOG, parser, "Patient Person ID for the Phone ID in PPPHO {} not found", phoneIdCell);
            return;
        }

        //if no number, then nothing to process - seems like in some cases the Millennium user doesn't delete
        //a phone number, but just amends it to have a blank number
        //Do this BEFORE we get the patient builder out, otherwise it never gets returned
        CsvCell numberCell = parser.getPhoneNumber();
        if (numberCell.isEmpty()) {
            return;
        }

        //get our patient resource builder
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
        if (patientBuilder == null) {
            return;
        }

        try {

            //by removing from the patient and re-adding, we're constantly changing the order of the
            //contact points, so attempt to reuse the existing one for the ID
            ContactPointBuilder contactPointBuilder = ContactPointBuilder.findOrCreateForId(patientBuilder, phoneIdCell);
            contactPointBuilder.reset(); //clear down as we've got a full phone record to replace all content with

            String number = numberCell.getString();

            //just append the extension on to the number
            CsvCell extensionCell = parser.getExtension();
            if (!extensionCell.isEmpty()) {
                number += " " + extensionCell.getString();
            }

            contactPointBuilder.setValue(number, numberCell, extensionCell);

            CsvCell startDate = parser.getBeginEffectiveDateTime();
            if (!startDate.isEmpty()) {
                Date d = NwlExtractsCsvHelper.parseDate(startDate);
                contactPointBuilder.setStartDate(d, startDate);
            }

            CsvCell endDate = parser.getEndEffectiveDateTime();
            //use this function to test the endDate cell, since it will have the Cerner end of time content
            if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
                Date d = NwlExtractsCsvHelper.parseDate(endDate);
                contactPointBuilder.setEndDate(d, endDate);
            }

            boolean isActive = true;
            if (contactPointBuilder.getContactPoint().hasPeriod()) {
                isActive = PeriodHelper.isActive(contactPointBuilder.getContactPoint().getPeriod());
            }

            CsvCell phoneTypeCell = parser.getPhoneTypeCode();
            CsvCell phoneTypeDescCell = NwlExtractsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_TYPE, phoneTypeCell);
            if(null!=phoneTypeDescCell) {
                String phoneTypeDesc = phoneTypeDescCell.getString();
                ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc, isActive);
                if(use!=null) {
                    contactPointBuilder.setUse(use, phoneTypeCell, phoneTypeDescCell);
                }

            CsvCell phoneMethodCell = parser.getContactMethodCode();
            if (!NwlExtractsCsvHelper.isEmptyOrIsZero(phoneMethodCell)) {

                CsvCell phoneMethodDescCell = NwlExtractsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_METHOD, phoneMethodCell);
                String phoneMethodDesc = phoneMethodDescCell.getString();

                ContactPoint.ContactPointSystem system = convertPhoneSystem(phoneMethodDesc);
                contactPointBuilder.setSystem(system, phoneTypeCell, phoneMethodCell, phoneMethodDescCell);

                 }
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

    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType, boolean isActive) throws Exception {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return ContactPoint.ContactPointUse.OLD;
        }

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        if (phoneType.equalsIgnoreCase("HOME")){
            return ContactPoint.ContactPointUse.HOME;
        } else if (phoneType.equalsIgnoreCase("BUSINESS")){
            return ContactPoint.ContactPointUse.WORK;
        } else if (phoneType.equalsIgnoreCase("MOBILE")){
            return ContactPoint.ContactPointUse.MOBILE;
        } else {
               //  throw new TransformException("Unsupported phone type [" + phoneType + "]");
            return null;
        }
    }

    private static ContactPoint.ContactPointSystem convertPhoneSystem(String method) throws Exception {

        //there are only four distinct methods in the code ref table
        //but there are cases where we don't have a method, so need to handle null
        if (method != null) {
            switch (method) {
                case "TEL":
                    return ContactPoint.ContactPointSystem.PHONE;
                case "FAX":
                    return ContactPoint.ContactPointSystem.FAX;
                case "TEXTPHONE":
                default:
                    return ContactPoint.ContactPointSystem.OTHER;
            }

        } else {
            //in the cases where we don't get a method, all the data received looked like phone numbers
            return ContactPoint.ContactPointSystem.PHONE;
        }
    }
}

