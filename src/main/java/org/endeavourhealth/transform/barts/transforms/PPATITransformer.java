package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsInternalIdDal;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PPATITransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformer.class);
    private static InternalIdDalI internalIdDalI = null;
    private static CernerCodeValueRefDalI cernerCodeValueRefDalI = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-MM HH:mm:ss.sss");

    public static void transform(String version,
                                 PPATI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatient(parser, fhirResourceFiler, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
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
                                     String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {


        if (internalIdDalI == null) {
            internalIdDalI = DalProvider.factoryInternalIdDal();
        }

        if (cernerCodeValueRefDalI == null) {
            cernerCodeValueRefDalI = DalProvider.factoryCernerCodeValueRefDal();
        }

        internalIdDalI.upsertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_MRN_MILLENNIUM_PERS_ID,
                parser.getLocalPatientId(), parser.getMillenniumPersonId());

        String mrn = parser.getMillenniumPersonId();

        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
        if (patientResourceId == null && !parser.isActive()) {
            //We don't already have the patient and it is not active so ignore it.
            return;
        }

        if (patientResourceId == null) {
            patientResourceId = createPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, mrn);
        }

        Patient fhirPatient = new Patient();
        fhirPatient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));

        fhirPatient.setId(patientResourceId.toString());

        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID,
                parser.getMillenniumPersonId()));

        if (!parser.isActive()) {
            // Patient is inactive and we already have them in the system so just set them to inactive and save the resource
            fhirPatient.setActive(false);
            PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonId()), fhirPatient);
        }

        fhirPatient.setActive(parser.isActive());

        fhirPatient.addIdentifier(IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY, FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON,
                parser.getLocalPatientId()));

        if (parser.getNhsNumber() != null && parser.getNhsNumber().length() > 0) {
            fhirPatient.addIdentifier(IdentifierHelper.createNhsNumberIdentifier(parser.getNhsNumber()));
        }

        if (parser.getNhsNumberStatus() != null && parser.getNhsNumberStatus().length() > 0) {

            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.NHS_NUMBER_STATUS,
                    Long.parseLong(parser.getActiveIndicator()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                CodeableConcept fhirCodeableConcept = null;
                //convert the String to one of the official statuses. If it can't be converted, insert free-text in the codeable concept
                NhsNumberVerificationStatus verificationStatus = convertNhsNumberVeriticationStatus(cernerCodeValueRef.getCodeDescTxt());
                if (verificationStatus != null) {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);

                } else {
                    fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(cernerCodeValueRef.getCodeDescTxt());
                }
                fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS,
                        fhirCodeableConcept));
            } else {
                LOG.warn("NHS Status code: " + parser.getActiveIndicator() + " not found in Code Value lookup");
            }
        }

        // TODO check for inactives and whether they are deletions with just the active flag populated and need to be handled differently
        if (parser.getActiveIndicator() != null && parser.getActiveIndicator().length() > 0) {
            fhirPatient.setActive(parser.isActive());
        }

        if (parser.getDateOfBirth() != null && parser.getDateOfBirth().toString().length() > 0) {
            Date dob = null;
            try {
                dob = formatDaily.parse(parser.getDateOfBirthAsString());
            } catch (ParseException ex) {
                dob = formatBulk.parse(parser.getDateOfBirthAsString());
            }
            fhirPatient.setBirthDate(dob);
        }

        if (parser.getGenderCode() != null && parser.getGenderCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.GENDER,
                    Long.parseLong(parser.getGenderCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(cernerCodeValueRef.getCodeMeaningTxt());
                fhirPatient.setGender(gender);
            } else {
                LOG.warn("Gender code: " + parser.getGenderCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getMaritalStatusCode() != null && parser.getMaritalStatusCode().length() > 0) {
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.MARITAL_STATUS,
                    Long.parseLong(parser.getMaritalStatusCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                MaritalStatus maritalStatus = convertMaritalStatus(cernerCodeValueRef.getCodeMeaningTxt());
                CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(maritalStatus);
                fhirPatient.setMaritalStatus(codeableConcept);
            } else {
                LOG.warn("Marital Status code: " + parser.getMaritalStatusCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getEthnicGroupCode() != null && parser.getEthnicGroupCode().length() > 0) {
            CodeableConcept ethnicGroup = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.ETHNIC_GROUP,
                    Long.parseLong(parser.getEthnicGroupCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                ethnicGroup.addCoding().setCode(parser.getEthnicGroupCode()).setSystem(FhirExtensionUri.PATIENT_ETHNICITY)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());
            } else {
                LOG.warn("Ethnic Group code: " + parser.getEthnicGroupCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getFirstLanguageCode() != null && parser.getFirstLanguageCode().length() > 0) {
            CodeableConcept languageConcept = new CodeableConcept();
            Patient.PatientCommunicationComponent fhirCommunication = fhirPatient.addCommunication();

            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.LANGUAGE,
                    Long.parseLong(parser.getFirstLanguageCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                languageConcept.addCoding().setCode(parser.getFirstLanguageCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());

                fhirCommunication.setLanguage(languageConcept);
                fhirCommunication.setPreferred(true);

                fhirPatient.addCommunication(fhirCommunication);
            } else {
                LOG.warn("Language code: " + parser.getFirstLanguageCode() + " not found in Code Value lookup");
            }
        }

        if (parser.getReligionCode() != null && parser.getReligionCode().length() > 0) {
            CodeableConcept religionConcept = new CodeableConcept();
            CernerCodeValueRef cernerCodeValueRef = cernerCodeValueRefDalI.getCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.RELIGION,
                    Long.parseLong(parser.getReligionCode()),
                    fhirResourceFiler.getServiceId());

            if (cernerCodeValueRef != null) {
                religionConcept.addCoding().setCode(parser.getReligionCode()).setSystem(FhirUri.CODE_SYSTEM_CERNER_CODE_ID)
                        .setDisplay(cernerCodeValueRef.getCodeDescTxt());

                fhirPatient.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.PATIENT_RELIGION, religionConcept));
            } else {
                LOG.warn("Religion code: " + parser.getReligionCode() + " not found in Code Value lookup");
            }
        }

        // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
        if (parser.getDeceasedDateTime() != null || parser.getDeceasedMethodCode() != null) {
            if (parser.getDeceasedDateTime() != null) {
                Date dod = null;
                try {
                    dod = formatDaily.parse(parser.getDeceasedDateTimeAsString());
                } catch (ParseException ex) {
                    dod = formatBulk.parse(parser.getDeceasedDateTimeAsString());
                }
                fhirPatient.setDeceased(new DateTimeType(dod));
            }  // 684730 = No for deceased method!
            else if (!parser.getDeceasedMethodCode().equals("0") || !parser.getDeceasedMethodCode().equals("684730")) {
                fhirPatient.setDeceased(new BooleanType(true));
            }
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
