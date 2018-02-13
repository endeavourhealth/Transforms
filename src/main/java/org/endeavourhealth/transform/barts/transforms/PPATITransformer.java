package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
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
    private static Long ethnicGroupCodeSet = 27L;
    private static Long languageGroupCodeSet = 36L;
    private static Long religionGroupCodeSet = 49L;
    private static Long maritalStatusGroupCodeSet = 38L;

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

        if (parser.getNhsNumber() != null && parser.getNhsNumber().length() > 0) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(parser.getNhsNumber()));
        }

        if (parser.getNhsNumberStatus() != null && parser.getNhsNumberStatus().length() > 0) {

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
        if (parser.getActiveIndicator() != null && parser.getActiveIndicator().length() > 0) {
            fhirPatient.setActive(parser.isActive());
        }

        if (parser.getDateOfBirth() != null && parser.getDateOfBirth().toString().length() > 0) {
            fhirPatient.setBirthDate(parser.getDateOfBirth());
        }

        if (parser.getGenderCode() != null && parser.getGenderCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(genderCodeSet, Long.parseLong(parser.getGenderCode()), fhirResourceFiler.getServiceId());
            Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
            fhirPatient.setGender(gender);
        }

        if (parser.getMaritalStatusCode() != null && parser.getMaritalStatusCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(maritalStatusGroupCodeSet, Long.parseLong(parser.getMaritalStatusCode()), fhirResourceFiler.getServiceId());
            MaritalStatus maritalStatus = convertMaritalStatus (cernerCodeValueRef.getCodeMeaningTxt());
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(maritalStatus);
            fhirPatient.setMaritalStatus(codeableConcept);
        }

        if (parser.getEthnicGroupCode() != null && parser.getEthnicGroupCode().length() > 0) {
            CodeableConcept ethnicGroup = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(ethnicGroupCodeSet, Long.parseLong(parser.getEthnicGroupCode()), fhirResourceFiler.getServiceId());
            ethnicGroup.addCoding().setCode(parser.getEthnicGroupCode()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY)
                    .setDisplay(cernerCodeValueRef.getCodeDescTxt());
        }

        if (parser.getFirstLanguageCode() != null && parser.getFirstLanguageCode().length() > 0) {
            CodeableConcept languageConcept = new CodeableConcept();
            Patient.PatientCommunicationComponent fhirCommunication = fhirPatient.addCommunication();
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(languageGroupCodeSet, Long.parseLong(parser.getFirstLanguageCode()), fhirResourceFiler.getServiceId());
            languageConcept.addCoding().setCode(parser.getFirstLanguageCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                    .setDisplay(cernerCodeValueRef.getCodeDescTxt());
            fhirCommunication.setLanguage(languageConcept);
            fhirCommunication.setPreferred(true);

            fhirPatient.addCommunication(fhirCommunication);
        }

        if (parser.getReligionCode() != null && parser.getReligionCode().length() > 0) {
            CodeableConcept religionConcept = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(religionGroupCodeSet, Long.parseLong(parser.getReligionCode()), fhirResourceFiler.getServiceId());
            religionConcept.addCoding().setCode(parser.getReligionCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                    .setDisplay(cernerCodeValueRef.getCodeDescTxt());

            fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_RELIGION, religionConcept));
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

    private static MaritalStatus convertMaritalStatus(String statusCode) {
        switch (statusCode) {
            case "DIVORCED": return MaritalStatus.DIVORCED;
            case "MARRIED": return MaritalStatus.MARRIED;
            case "LGL_SPRTN": return MaritalStatus.LEGALLY_SEPARATED;
            case "SINGLE": return MaritalStatus.NEVER_MARRIED;
            case "UNKNOWN": return null;
            case "WIDOW": return MaritalStatus.WIDOWED;
            case "LIFE_PTNR": return MaritalStatus.DOMESTIC_PARTNER;
            default: return null;
        }
    }
}
