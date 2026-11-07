package org.endeavourhealth.transform.nwlextracts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.nwlextracts.CodeValueSet;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCodeableConceptHelper;
import org.endeavourhealth.transform.nwlextracts.NwlExtractsCsvHelper;
import org.endeavourhealth.transform.nwlextracts.schema.PPADD;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPADDTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDTransformer.class);

    private static final String EMAIL_ID_PREFIX = "PPADD";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                processRecord((PPADD) parser, fhirResourceFiler, csvHelper,sourceSystem);
            }
        }
    }


    public static void processRecord(PPADD parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper, String sourceSystem) throws Exception {

        CsvCell addressIdCell = parser.getMillenniumAddressId();
        CsvCell personIdCell = parser.getPersonId();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            //There are a small number of cases where all the fields are empty (including person ID) but in all examined
            //cases, we've never previously received a valid record, so can just ignore them
            if (!personIdCell.isEmpty()) {
                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
                if (patientBuilder != null) {

                    AddressBuilder.removeExistingAddressById(patientBuilder, addressIdCell.getString());

                    //the address may have been saved as a Contact Point if it was an email address
                    ContactPointBuilder.removeExistingContactPointById(patientBuilder, EMAIL_ID_PREFIX + addressIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
                }
            }
            TransformWarnings.log(LOG, parser, "Patient Person ID for the Address ID in PPADD {} not found", addressIdCell);
            return;
        }

        CsvCell typeCell = parser.getAddressTypeCode();
        if (NwlExtractsCsvHelper.isEmptyOrIsZero(typeCell)) {
            return;
        }

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell postcode = parser.getPostcode();

        //ignore any empty records
        if (line1.isEmpty()
                && line2.isEmpty()
                && line3.isEmpty()
                && line4.isEmpty()
                && city.isEmpty()
                && postcode.isEmpty()) {
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell,sourceSystem);
        if (patientBuilder == null) {
            //LOG.trace("No patient builder, so skipping");
            return;
        }

        try {

            CsvCell typeDescCell = NwlExtractsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
           if(null!=typeDescCell) {
               String typeDesc = typeDescCell.getString();

            //a very small number of patients (two, that I've seen) have an address recorded
            //with type "e-mail", but also have this email duplicated in the PPPHO file (where email is normally recorded)
            //so ignore any PPADD records for emails
            //this must be done BEFORE getting the patient builder, otherwise we fail to return it
            if (typeDesc.equalsIgnoreCase("e-mail")) {
                processRecordAsEmail(patientBuilder, parser, fhirResourceFiler, csvHelper);

               } else {
                   processRecordAsAddress(patientBuilder, parser, fhirResourceFiler, csvHelper);
               }
           }
        } finally {
            //no need to save the resource now, as all patient resources are saved at the end of the PP... files
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
            //LOG.trace("Returned to patient cache with person ID " + personIdCell + " with " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");
        }
    }

    private static void processRecordAsAddress(PatientBuilder patientBuilder, PPADD parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper) throws Exception {

        CsvCell addressIdCell = parser.getMillenniumAddressId();

        //by always removing and re-adding addresses, we're constantly changing the order, so attempt to re-use them
        AddressBuilder addressBuilder = AddressBuilder.findOrCreateForId(patientBuilder, addressIdCell);
        addressBuilder.reset(); //we always fully re-populate, so clear down first

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell county = parser.getCountyText();
        CsvCell postcode = parser.getPostcode();


        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(line1.getString(), line1);
        addressBuilder.addLine(line2.getString(), line2);
        addressBuilder.addLine(line3.getString(), line3);
        addressBuilder.addLine(line4.getString(), line4);
        addressBuilder.setCity(city.getString(), city);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell startDate = parser.getBeginEffectiveDate();
        if (!NwlExtractsCsvHelper.isEmptyOrIsStartOfTime(startDate)) { //there are cases with empty start dates
            Date d = NwlExtractsCsvHelper.parseDate(startDate);
            addressBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDate();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = NwlExtractsCsvHelper.parseDate(endDate);
            addressBuilder.setEndDate(d, endDate);
        }

        boolean isActive = true;
        if (addressBuilder.getAddressCreated().hasPeriod()) {
            isActive = PeriodHelper.isActive(addressBuilder.getAddressCreated().getPeriod());
        }

        CsvCell typeCell = parser.getAddressTypeCode();
        CsvCell typeDescCell = NwlExtractsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
        String typeDesc = typeDescCell.getString();

        Address.AddressUse use = convertAddressUse(typeDesc, isActive);
        if (use != null) {
            addressBuilder.setUse(use, typeCell, typeDescCell);
        }

        //remove any instance of the address added by the ADT feed
        Address addressCreated = addressBuilder.getAddressCreated();
        removeExistingAddressWithoutIdByValue(patientBuilder, addressCreated);
    }

    private static void processRecordAsEmail(PatientBuilder patientBuilder, PPADD parser, FhirResourceFiler fhirResourceFiler, NwlExtractsCsvHelper csvHelper) throws Exception {

        CsvCell addressIdCell = parser.getMillenniumAddressId();

        //to avoid mix-up between PPPHO and PPADD telecoms, prefix the PPADD one with some text
        CsvCell emailAddressIdCell = CsvCell.factoryWithNewValue(addressIdCell, EMAIL_ID_PREFIX + addressIdCell.getString());

        ContactPointBuilder contactPointBuilder = ContactPointBuilder.findOrCreateForId(patientBuilder, emailAddressIdCell);
        contactPointBuilder.reset(); //clear down as we've got a full phone record to replace all content with

        CsvCell emailAddressCell = parser.getAddressLine1(); //line 1 contains the email address
        contactPointBuilder.setValue(emailAddressCell.getString(), emailAddressCell);

        CsvCell startDate = parser.getBeginEffectiveDate();
        if (!startDate.isEmpty()) {
            Date d = NwlExtractsCsvHelper.parseDate(startDate);
            contactPointBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDate();
        if (!NwlExtractsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = NwlExtractsCsvHelper.parseDate(endDate);
            contactPointBuilder.setEndDate(d, endDate);
        }

        boolean isActive = true;
        if (contactPointBuilder.getContactPoint().hasPeriod()) {
            isActive = PeriodHelper.isActive(contactPointBuilder.getContactPoint().getPeriod());
        }

        if (!isActive) {
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.OLD);
        } else {
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
        }

        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);

        //and remove any instance of this email address created by the ADT feed
        ContactPoint newOne = contactPointBuilder.getContactPoint();
        PPPHOTransformer.removeExistingContactPointWithoutIdByValue(patientBuilder, newOne);

    }

    private static Address.AddressUse convertAddressUse(String typeDesc, boolean isActive) throws TransformException {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return Address.AddressUse.OLD;
        }

        //NOTE there are 20+ address types in CVREF, but only the types known to be used are mapped below
        if (typeDesc.equalsIgnoreCase("home")) {
            return Address.AddressUse.HOME;
        } else if (typeDesc.equalsIgnoreCase("temporary")) {
            return Address.AddressUse.TEMP;
        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressType(..) too
            // throw new TransformException("Unhandled type [" + typeDesc + "]");
            return null;
        }
    }

    public static void removeExistingAddressWithoutIdByValue(PatientBuilder patientBuilder, Address check) {

        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasAddress()) {
            return;
        }

        List<Address> addresses = patient.getAddress();
        List<Address> duplicates = AddressHelper.findMatches(check, addresses);

        for (Address duplicate: duplicates) {

            //if this name has an ID it was created by this data warehouse feed, so don't try to remove it
            if (duplicate.hasId()) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            addresses.remove(duplicate);
        }
    }
}