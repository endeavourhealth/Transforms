package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPATITransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);
    private static InternalIdDalI internalIdDalI = null;
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;
    private static Long nhsNumberStatusCodeSet = 29882L;
    private static Long genderCodeSet = 57L;

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

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        String mrn = internalIdDalI.getDestinationId(fhirResourceFiler.getServiceId(), "PATIENT", parser.getMillenniumPersonId());

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));
        fhirPatient.setId(mrn);

        if (parser.getNhsNumber() != null) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(parser.getNhsNumber()));
        }

        if (parser.getNhsNumberStatus() != null) {

            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(nhsNumberStatusCodeSet, Long.parseLong(parser.getActiveIndicator()), fhirResourceFiler.getServiceId());
            if (cernerCodeValueRef != null) {
                CodeableConcept fhirCodeableConcept = null;
                //convert the String to one of the official statuses. If it can't be converted, insert free-text in the codeable concept
                NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerCodeValueRef.getCodeDescTxt());
                if (verificationStatus != null) {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);

                } else {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(cernerCodeValueRef.getCodeDescTxt());
                }
                fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, fhirCodeableConcept));
            }
        }

        // TODO check for inactives and whether they are deletions with just the active flag populated and need to be handled differently
        if (parser.getActiveIndicator() != null) {
            fhirPatient.setActive(parser.isActive());
        }

        if (parser.getDateOfBirth() != null) {
            fhirPatient.setBirthDate(parser.getDateOfBirth());
        }

        if (parser.getGenderCode() != null) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(genderCodeSet, Long.parseLong(parser.getGenderCode()), fhirResourceFiler.getServiceId());
            Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
            fhirPatient.setGender(gender);
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

    private static NhsNumberVerificationStatus convertNhsNumberVeriticationStatus(String nhsNumberStatus) {
        switch (nhsNumberStatus) {
            case "Present and verified":
                return NhsNumberVerificationStatus.PRESENT_AND_VERIFIED;
            case "Needs Resolution":
                return NhsNumberVerificationStatus.TRACE_NEEDS_TO_BE_RESOLVED;
            case "Present not Traced":
                return NhsNumberVerificationStatus.PRESENT_BUT_NOT_TRACED;
            case "Traced Attempted":
                return NhsNumberVerificationStatus.TRACE_ATTEMPTED_NO_MATCH;
            case "Traced Required":
                return NhsNumberVerificationStatus.TRACE_REQUIRED;
            case "Trace in Progress":
                return NhsNumberVerificationStatus.TRACE_IN_PROGRESS;
            case "Trace not Required":
                return NhsNumberVerificationStatus.NUMBER_NOT_PRESENT_NO_TRACE_REQUIRED;
            case "Trace Postponed":
                return NhsNumberVerificationStatus.TRACE_POSTPONED;
            default:
                return null;
        }
    }
}
