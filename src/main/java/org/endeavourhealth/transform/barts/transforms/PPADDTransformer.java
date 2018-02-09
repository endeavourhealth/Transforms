package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPADDTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);

    public static void transform(String version,
                                 PPADD parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

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
                                         EmisCsvHelper csvHelper,
                                         String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");

        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);


        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME,
                parser.getAddressLine1(), parser.getAddressLine2(), parser.getAddressLine3(),
                parser.getAddressLine4(), parser.getCountyText(), parser.getPostcode());

        /*
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};


        */
    }
}