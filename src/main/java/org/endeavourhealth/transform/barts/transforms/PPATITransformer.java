package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPATITransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);
    private static InternalIdDalI internalIdDalI = null;

    public static void transform(String version,
                                 PPATI parser,
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
                    createPatient(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPATI parser) {
        return null;
    }


    public static void createPatient(PPATI parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     EmisCsvHelper csvHelper,
                                     String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        if (internalIdDalI == null) {
            internalIdDalI = DalProvider.factoryInternalIdDal();
        }

        String mrn = internalIdDalI.getDestinationId(fhirResourceFiler.getServiceId(), "PATIENT", parser.getMillenniumPersonId());

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));
        fhirPatient.setId(mrn);

        if (parser.getNhsNumber() != null) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(parser.getNhsNumber()));
        }
        // TODO get the verification code and process
        //fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, parser.getActiveIndicator()));


        // TODO check for inactives and whether they are deletions with just the active flag populated and need to be handled differently
        if (parser.getActiveIndicator() != null) {
            fhirPatient.setActive(parser.isActive());
        }

        if (parser.getDateOfBirth() != null) {
            fhirPatient.setBirthDate(parser.getDateOfBirth());
        }

        // TODO get the gender code and process
        if (parser.getGenderCode() != null) {
            //fhirPatient.setGender()
        }

        // TODO get the marital status code and process
        if (parser.getMaritalStatusCode() != null) {
            //fhirPatient.setMaritalStatus();
        }

        // TODO get the ethnic group code and process
        if (parser.getEthnicGroupCode() != null) {
            //fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_ETHNICITY, ));
        }

        // TODO get the first language code and process
        if (parser.getFirstLanguageCode() != null) {
            //fhirPatient.setLanguage();
        }

        // TODO get the religion code and process
        if (parser.getReligionCode() != null) {
            //fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_RELIGION, ));
        }

        if (parser.getDeceasedDateTime() != null || parser.getDeceasedMethodCode() != null) {
            fhirPatient.setDeceased(new DateTimeType(parser.getDeceasedDateTime()));
            // TODO check if both are set and if not only add one.  Need to process the deceased method code
            //fhirPatient.setDeceased(new BooleanType())
        }


        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonId()), fhirPatient);


        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");

        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);
        /*
        CodeableConcept ethnicGroup = null;
        if (!Strings.isNullOrEmpty(parser.getEthnicGroupCode())) {
            ethnicGroup = new CodeableConcept();
            ethnicGroup.addCoding().setCode(parser.getEthnicCategory()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY).setDisplay(getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
            //LOG.debug("Ethnic group:" + parser.getEthnicCategory() + "==>" + getSusEthnicCategoryDisplay(parser.getEthnicCategory()));
        }*/

    }
}
