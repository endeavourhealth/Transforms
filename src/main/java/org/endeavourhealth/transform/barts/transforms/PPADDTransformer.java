package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.emis.emisopen.transforms.common.AddressConverter;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Patient;
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
                createPatientAddress((PPADD) parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void createPatientAddress(PPADD parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell addressIdCell = parser.getMillenniumAddressId();

        //if non-active (i.e. deleted) we should REMOVE the address, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPADDPreTransformer.PPADD_ID_TO_PERSON_ID, addressIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                if (patientBuilder != null) {
                    AddressBuilder.removeExistingAddress(patientBuilder, addressIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        CsvCell personIdCell = parser.getPersonId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell county = parser.getCountyText();
        CsvCell postcode = parser.getPostcode();

        //we always fully re-create the address, so remove it from the patient
        AddressBuilder.removeExistingAddress(patientBuilder, addressIdCell.getString());

        //and remove any instance of the address added by the ADT feed
        removeExistingAddressWithoutIdByValue(patientBuilder, line1, line2, line3, line4, city, county, postcode);

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
        if (!startDate.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(startDate);
            addressBuilder.setStartDate(d, startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDate)) {
            Date d = BartsCsvHelper.parseDate(endDate);
            addressBuilder.setEndDate(d, endDate);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }

    private static void removeExistingAddressWithoutIdByValue(PatientBuilder patientBuilder, CsvCell line1, CsvCell line2, CsvCell line3, CsvCell line4, CsvCell city, CsvCell county, CsvCell postcode) {
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
    }

}