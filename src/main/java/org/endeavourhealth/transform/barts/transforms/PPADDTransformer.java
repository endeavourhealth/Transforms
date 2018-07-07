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
import org.hl7.fhir.instance.model.Address;
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

        //we always fully re-create the address, so remove it from the patient
        AddressBuilder.removeExistingAddress(patientBuilder, addressIdCell.getString());

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell county = parser.getCountyText();
        CsvCell postcode = parser.getPostcode();

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

}