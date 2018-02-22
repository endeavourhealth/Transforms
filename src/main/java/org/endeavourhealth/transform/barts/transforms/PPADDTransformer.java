package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPADDTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDTransformer.class);

    public static void transform(String version,
                                 PPADD parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatientAddress(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPADD parser) {
        return null;
    }

    public static void createPatientAddress(PPADD parser,
                                            FhirResourceFiler fhirResourceFiler,
                                            BartsCsvHelper csvHelper,
                                            String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        //if non-active, we should REMOVE the address
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            //TODO - need to REMOVE address from patient

            return;
        }

        //TODO - handle deltas where we're updating an existing address

        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell county = parser.getCountyText();
        CsvCell postcode = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
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
            addressBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell endDate = parser.getEndEffectiveDater();
        if (!endDate.isEmpty()) {
            addressBuilder.setEndDate(endDate.getDate(), endDate);
        }
    }

    /*public static void createPatientAddress(PPADD parser,
                                         FhirResourceFiler fhirResourceFiler,
                                        BartsCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        Patient fhirPatient = PatientResourceCache.getPatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()));

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        if (fhirPatient == null) {
            if (parser.isActive()) {
                LOG.warn("Patient Resource Not Found In Cache: " + parser.getMillenniumPersonIdentifier());
            } else {
                return;
            }
        }

        // Patient Address
        Patient.ContactComponent fhirContactComponent = new Patient.ContactComponent();


        Address fhirRelationAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddressLine1(),
                parser.getAddressLine2(), parser.getAddressLine3(), parser.getAddressLine4(), parser.getCountyText(), parser.getPostcode());

        fhirContactComponent.setAddress(fhirRelationAddress);

    }*/
}