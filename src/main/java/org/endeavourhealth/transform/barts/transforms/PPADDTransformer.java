package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.emisopen.transforms.common.AddressConverter;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPADDTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                processRecord((PPADD) parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void processRecord(PPADD parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell addressIdCell = parser.getMillenniumAddressId();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPADDPreTransformer.PPADD_ID_TO_PERSON_ID, addressIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr));
                if (patientBuilder != null) {
                    AddressBuilder.removeExistingAddressById(patientBuilder, addressIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        CsvCell personIdCell = parser.getPersonId();
        CsvCell typeCell = parser.getAddressTypeCode();
        CsvCell typeDescCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
        String typeDesc = typeDescCell.getString();

        //a very small number of patients (two, that I've seen) have an address recorded
        //with type "e-mail", but also have this email duplicated in the PPPHO file (where email is normally recorded)
        //so ignore any PPADD records for emails
        //this must be done BEFORE getting the patient builder, otherwise we fail to return it
        if (typeDesc.equalsIgnoreCase("e-mail")) {
            TransformWarnings.log(LOG, csvHelper, "Skipping PPADD {} for person {} because type is {}", addressIdCell, personIdCell, typeDesc);
            return;
        }

        //LOG.trace("Processing PPADD " + addressIdCell.getString() + " for Person ID " + personIdCell.getString());
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            //LOG.trace("No patient builder, so skipping");
            return;
        }

        try {
            //LOG.trace("FHIR resource = " + patientBuilder.toString() + " starts with " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");

            CsvCell line1 = parser.getAddressLine1();
            CsvCell line2 = parser.getAddressLine2();
            CsvCell line3 = parser.getAddressLine3();
            CsvCell line4 = parser.getAddressLine4();
            CsvCell city = parser.getCity();
            CsvCell county = parser.getCountyText();
            CsvCell postcode = parser.getPostcode();

            //we always fully re-create the address, so remove it from the patient
            boolean removedExisting = AddressBuilder.removeExistingAddressById(patientBuilder, addressIdCell.getString());
            //LOG.trace("Removed existing = " + removedExisting + " leaving " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");

            AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
            addressBuilder.setId(addressIdCell.getString(), addressIdCell);
            addressBuilder.setUse(Address.AddressUse.HOME);
            addressBuilder.addLine(line1.getString(), line1);
            addressBuilder.addLine(line2.getString(), line2);
            addressBuilder.addLine(line3.getString(), line3);
            addressBuilder.addLine(line4.getString(), line4);
            addressBuilder.setTown(city.getString(), city);
            addressBuilder.setDistrict(county.getString(), county);
            addressBuilder.setPostcode(postcode.getString(), postcode);

            CsvCell startDate = parser.getBeginEffectiveDate();
            if (!BartsCsvHelper.isEmptyOrIsStartOfTime(startDate)) { //there are cases with empty start dates
                Date d = BartsCsvHelper.parseDate(startDate);
                addressBuilder.setStartDate(d, startDate);
            }

            CsvCell endDate = parser.getEndEffectiveDater();
            //use this function to test the endDate cell, since it will have the Cerner end of time content
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
                Date d = BartsCsvHelper.parseDate(endDate);
                addressBuilder.setEndDate(d, endDate);
            }

            boolean isActive = true;
            if (addressBuilder.getAddressCreated().hasPeriod()) {
                isActive = PeriodHelper.isActive(addressBuilder.getAddressCreated().getPeriod());
            }

            Address.AddressUse use = convertAddressUse(typeDesc, isActive);
            if (use != null) {
                addressBuilder.setUse(use, typeCell, typeDescCell);
            }

            Address.AddressType type = convertAddressType(typeDesc);
            if (type != null) {
                addressBuilder.setType(type, typeCell, typeDescCell);
            }

            //LOG.trace("Added new address, FHIR now has " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");

            //remove any instance of the address added by the ADT feed
            Address addressCreated = addressBuilder.getAddressCreated();
            removeExistingAddressWithoutIdByValue(patientBuilder, addressCreated);
            //LOG.trace("Removed duplicate from ADT feed, and FHIR now has " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");

        } finally {
            //no need to save the resource now, as all patient resources are saved at the end of the PP... files
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
            //LOG.trace("Returned to patient cache with person ID " + personIdCell + " with " + ((Patient) patientBuilder.getResource()).getAddress().size() + " addresses");
        }
    }

    private static Address.AddressType convertAddressType(String typeDesc) throws TransformException {

        //NOTE we only use address type if it's explicitly known to be a mailing address
        if (typeDesc.equalsIgnoreCase("mailing")) {
            return Address.AddressType.POSTAL;

        } else if (typeDesc.equalsIgnoreCase("Birth Address")
                || typeDesc.equalsIgnoreCase("home")
                || typeDesc.equalsIgnoreCase("business")
                || typeDesc.equalsIgnoreCase("temporary")
                || typeDesc.equalsIgnoreCase("Prevous Address")) { //note the wrong spelling is in the Cerner data CVREF file
            return null;

        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressUse(..) too
            throw new TransformException("Unhandled type [" + typeDesc + "]");
        }
    }

    private static Address.AddressUse convertAddressUse(String typeDesc, boolean isActive) throws TransformException {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return Address.AddressUse.OLD;
        }

        //NOTE there are 20+ address types in CVREF, but only the types known to be used are mapped below
        if (typeDesc.equalsIgnoreCase("Birth Address")
                || typeDesc.equalsIgnoreCase("home")
                || typeDesc.equalsIgnoreCase("mailing")) {
            return Address.AddressUse.HOME;

        } else if (typeDesc.equalsIgnoreCase("business")) {
            return Address.AddressUse.WORK;

        } else if (typeDesc.equalsIgnoreCase("temporary")) {
            return Address.AddressUse.TEMP;

        } else if (typeDesc.equalsIgnoreCase("Prevous Address")) { //note the wrong spelling is in the Cerner data CVREF file
            return Address.AddressUse.OLD;

        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressType(..) too
            throw new TransformException("Unhandled type [" + typeDesc + "]");
        }
    }

    public static void removeExistingAddressWithoutIdByValue(PatientBuilder patientBuilder, Address check) {
        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasAddress()) {
            return;
        }

        List<Address> addresses = patient.getAddress();
        for (int i=addresses.size()-1; i>=0; i--) {
            Address address = addresses.get(i);

            //if this one has an ID it was created by this data warehouse feed, so don't try to remove it
            if (address.hasId()) {
                continue;
            }

            boolean matches = true;

            if (address.hasLine()) {
                for (StringType line: address.getLine()) {
                    if (!AddressConverter.hasLine(check, line.toString())) {
                        matches = false;
                        break;
                    }
                }
            }

            if (address.hasCity()) {
                if (!AddressConverter.hasCity(check, address.getCity())) {
                    matches = false;
                }
            }

            if (address.hasDistrict()) {
                if (!AddressConverter.hasDistrict(check, address.getDistrict())) {
                    matches = false;
                }
            }

            if (address.hasPostalCode()) {
                String postcode = address.getPostalCode();
                if (!AddressConverter.hasPostcode(check, postcode)) {
                    matches = false;
                }
            }

            //if we make it here, it's a duplicate and should be removed
            if (matches) {
                addresses.remove(i);
            }
        }
    }

    /*private static void removeExistingAddressWithoutIdByValue(PatientBuilder patientBuilder, CsvCell line1, CsvCell line2, CsvCell line3, CsvCell line4, CsvCell city, CsvCell county, CsvCell postcode) {
        Patient patient = (Patient)patientBuilder.getResource();
        if (!patient.hasAddress()) {
            return;
        }

        List<Address> addresses = patient.getAddress();
        for (int i=addresses.size()-1; i>=0; i--) {
            Address address = addresses.get(i);

            //if this one has an ID it was created by this data warehouse feed, so don't try to remove it
            if (address.hasId()) {
                continue;
            }

            if (!line1.isEmpty()
                    && !AddressConverter.hasLine(address, line1.getString())) {
                continue;
            }

            if (!line2.isEmpty()
                    && !AddressConverter.hasLine(address, line2.getString())) {
                continue;
            }

            if (!line3.isEmpty()
                    && !AddressConverter.hasLine(address, line3.getString())) {
                continue;
            }

            if (!line4.isEmpty()
                    && !AddressConverter.hasLine(address, line4.getString())) {
                continue;
            }

            if (!city.isEmpty()
                    && !AddressConverter.hasCity(address, city.getString())) {
                continue;
            }

            if (!county.isEmpty()
                    && !AddressConverter.hasDistrict(address, county.getString())) {
                continue;
            }

            if (!postcode.isEmpty()
                    && !AddressConverter.hasPostcode(address, postcode.getString())) {
                continue;
            }

            //if we make it here, it's a duplicate and should be removed
            addresses.remove(i);
        }
    }*/

}