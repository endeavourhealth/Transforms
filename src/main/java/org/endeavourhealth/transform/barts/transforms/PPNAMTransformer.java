package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.transform.barts.schema.PPNAM;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPNAMTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);

    public static void transform(String version,
                                 PPNAM parser,
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
                    createPatientName(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPNAM parser) {
        return null;
    }


    public static void createPatientName(PPNAM parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     EmisCsvHelper csvHelper,
                                     String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");

        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);


        // Patient Name
        HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(
                HumanName.NameUse.OFFICIAL,
                parser.getTitle(), parser.getFirstName(), parser.getMiddleName(),
                parser.getLastName(), parser.getSuffix());

        /*
        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME,
                parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());


        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};

        CodeableConcept ethnicGroup = null;
        if (parser.getEthnicCategory() != null && parser.getEthnicCategory().length() > 0) {
            ethnicGroup = new CodeableConcept();
            ethnicGroup.addCoding().setCode(parser.getEthnicCategory()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY).setDisplay(getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
            //LOG.debug("Ethnic group:" + parser.getEthnicCategory() + "==>" + getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
        }
        */
    }

    private static String parseTitleAndPrefix(String title, String prefix) throws Exception {

        if (Strings.isNullOrEmpty(title) && Strings.isNullOrEmpty(prefix)) {
            return "";
        } else if (Strings.isNullOrEmpty(title) && !Strings.isNullOrEmpty(prefix)) {
            return prefix;
        } else if (!Strings.isNullOrEmpty(title) && Strings.isNullOrEmpty(prefix)) {
            return title;
        } else {
            if (title.toLowerCase().equals(prefix.toLowerCase())) {
                return prefix;
            } else {
                return processKnownDuplicateTitles(title, prefix);
            }

        }
    }

    private static String processKnownDuplicateTitles(String title, String prefix) {

        if (title.toLowerCase().replace(".", "").equals("master") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("ms") && prefix.toLowerCase().replace(".", "").equals("miss")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("mst") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("mister") && prefix.toLowerCase().replace(".", "").equals("mr")) {
            return prefix;
        }

        if (title.toLowerCase().replace(".", "").equals("m")) {
            return prefix;
        }

        return prefix + " " + title ;
    }
}