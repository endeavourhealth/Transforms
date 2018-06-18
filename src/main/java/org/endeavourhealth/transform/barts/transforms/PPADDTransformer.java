package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPADD;
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
                try {
                    createPatientAddress((PPADD) parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    public static void createPatientAddress(PPADD parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        //we always fully re-create the address, so remove it from the patient
        CsvCell addressIdCell = parser.getMillenniumAddressId();
        AddressBuilder.removeExistingAddress(patientBuilder, addressIdCell.getString());

        //if non-active, we should REMOVE the address, which we've already done, so just return out
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

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
    }

}