package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsCernerCodeValueRefDal;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.PatientResourceCache;
import org.endeavourhealth.transform.barts.schema.PPALI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPALITransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPALITransformer.class);

    public static void transform(String version,
                                 PPALI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createPatientAlias(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static String validateEntry(PPALI parser) {
        return null;
    }

    public static void createPatientAlias(PPALI parser,
                                          FhirResourceFiler fhirResourceFiler,
                                          BartsCsvHelper csvHelper,
                                          String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        //if the alias is empty, there's nothing to add
        CsvCell aliasCell = parser.getAlias();
        if (aliasCell.isEmpty()) {
            return;
        }

        CsvCell milleniumId = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumId, csvHelper);

        // If we can't find a patient resource from a previous PPATI file, throw an exception but if the line is inactive then just ignore it
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            //TODO - need to support DELETING alias of existing patient

            return;
        }

        //TODO - need to handle receving an update to an existing alias (e.g. end date is set)

        // Patient Alias (these are all secondary as MRN and NHS are added in PPATI
        CsvCell aliasTypeCodeCell = parser.getAliasTypeCode();
        CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                                                                    RdbmsCernerCodeValueRefDal.ALIAS_TYPE,
                                                                    aliasTypeCodeCell.getLong(),
                                                                    fhirResourceFiler.getServiceId());

        String aliasSystem = FhirUri.IDENTIFIER_SYSTEM_CERNER_OTHER_PERSON_ID;

        if (cernerCodeValueRef != null) {
            aliasSystem = convertAliasCode(cernerCodeValueRef.getCodeMeaningTxt());
        } else {
            // LOG.warn("Alias Type code: " + parser.getAliasTypeCode() + " not found in Code Value lookup");
        }

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.addIdentifier();
        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilder.setSystem(aliasSystem, aliasTypeCodeCell);
        identifierBuilder.setValue(aliasCell.getString(), aliasCell);

        CsvCell startDateCell = parser.getBeginEffectiveDate();
        if (!startDateCell.isEmpty()) {
            identifierBuilder.setStartDate(startDateCell.getDate(), startDateCell);
        }

        CsvCell endDateCell = parser.getEndEffectiveDate();
        if (!endDateCell.isEmpty()) {
            identifierBuilder.setEndDate(startDateCell.getDate(), startDateCell);
        }

    }

    /*public static void createPatientAlias(PPALI parser,
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

        // Patient Alias (these are all secondary as MRN and NHS are added in PPATI
        if (parser.getAlias() != null && parser.getAlias().length() > 0) {

            CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                    RdbmsCernerCodeValueRefDal.ALIAS_TYPE,
                    Long.parseLong(parser.getAliasTypeCode()),
                    fhirResourceFiler.getServiceId());

            String aliasCode = FhirUri.IDENTIFIER_SYSTEM_CERNER_OTHER_PERSON_ID;

            if (cernerCodeValueRef != null) {
                aliasCode = convertAliasCode(cernerCodeValueRef.getCodeMeaningTxt());
            } else {
                // LOG.warn("Alias Type code: " + parser.getAliasTypeCode() + " not found in Code Value lookup");
            }

            Identifier identifier = IdentifierHelper.createIdentifier(Identifier.IdentifierUse.SECONDARY,
                    aliasCode, parser.getAlias());

            if (parser.getBeginEffectiveDate() != null || parser.getEndEffectiveDater() != null) {
                Period fhirPeriod = PeriodHelper.createPeriod(parser.getBeginEffectiveDate(), parser.getEndEffectiveDater());
                identifier.setPeriod(fhirPeriod);
            }

            fhirPatient.addIdentifier(identifier);
        }

        PatientResourceCache.savePatientResource(Long.parseLong(parser.getMillenniumPersonIdentifier()), fhirPatient);

    }*/

    private static String convertAliasCode(String statusCode) {
        switch (statusCode) {
            case "INTPERSID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON;
            case "CMRN": return FhirUri.IDENTIFIER_SYSTEM_CERNER_COMMUNITY_MEDICAL_RECORD;
            case "DONORID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_DONOR_ID;
            case "DNRN": return FhirUri.IDENTIFIER_SYSTEM_CERNER_DONOR_NUMBER;
            case "DRLIC": return FhirUri.IDENTIFIER_SYSTEM_CERNER_DRIVING_LICENSE_NUMBER;
            case "FILLER ORDER": return FhirUri.IDENTIFIER_SYSTEM_CERNER_FILLER_ORDER;
            case "HISTCMRN": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_CMRN;
            case "HISTMRN": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_MRN;
            case "HNASYSID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HNA_PAT_SYS_ID;
            case "MRN": return FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID;
            case "NHIN": return FhirUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "NMDP": return FhirUri.IDENTIFIER_SYSTEM_CERNER_NMDP;
            case "PASSPORT": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PASSPORT_NUMBER;
            case "PATID_V4": return FhirUri.IDENTIFIER_SYSTEM_CERNER_V400_OCF_PATIENT;
            case "PERSON NAME": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PERSON_NAME;
            case "PLACER ORDER": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PLACER_ORDER;
            case "PRN": return FhirUri.IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID;
            case "SSN": return FhirUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "SHIN": return FhirUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "UNOS": return FhirUri.IDENTIFIER_SYSTEM_CERNER_UNOS;
            case "HNAPERSONID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HNA_PERSON_ID;
            case "MILITARYID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_MILITARY_ID;
            case "OTHER": return FhirUri.IDENTIFIER_SYSTEM_CERNER_OTHER_PERSON_ID;
            case "REF_MRN": return FhirUri.IDENTIFIER_SYSTEM_CERNER_REFERRING_MRN;
            case "OUTREACH": return FhirUri.IDENTIFIER_SYSTEM_CERNER_OUTREACH_PERSON;
            case "PBSID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PBS_PATIENT;
            case "ACCOUNT": return FhirUri.IDENTIFIER_SYSTEM_CERNER_ACCOUNT_NUMBER;
            case "MESSAGING": return FhirUri.IDENTIFIER_SYSTEM_CERNER_SECURE_MESSAGING;
            case "HICR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HIC_RECIPIENT;
            case "INTLD": return FhirUri.IDENTIFIER_SYSTEM_CERNER_INTERNATIONAL_DONOR;
            case "NMDPD": return FhirUri.IDENTIFIER_SYSTEM_CERNER_NMDP_DONOR;
            case "NMDPR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_NMDP_RECIPIENT;
            case "OPOD": return FhirUri.IDENTIFIER_SYSTEM_CERNER_OPO_DONOR;
            case "OPOR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_OPO_RECIPIENT;
            case "PXR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PX_ID;
            case "UNOSD": return FhirUri.IDENTIFIER_SYSTEM_CERNER_UNOS_DONOR;
            case "Cephalosporins": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CEPHALOSPORINS;
            case "Penicillins": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PENICILLINS;
            case "PASSWORD": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PASSWORD;
            case "VERSION": return FhirUri.IDENTIFIER_SYSTEM_CERNER_VERSION;
            case "MEMBERNBR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_MEMBER_NUMBER;
            case "Cortisol level": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "ZZZZCortisol level": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "RNJ R Gyne Secs GynaeObs Secs": return FhirUri.IDENTIFIER_SYSTEM_CERNER_RNJ;
            case "RNJ R Gyne SMgr GynaeService Mgr": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "RNJ R NEUR ARK  Kelso": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "PERSONMEDIA": return FhirUri.IDENTIFIER_SYSTEM_CERNER_PERSON_MEDIA_ALIAS;
            case "INSC": return FhirUri.IDENTIFIER_SYSTEM_CERNER_NATIONAL_PATIENT_ID;
            case "MPI": return FhirUri.IDENTIFIER_SYSTEM_CERNER_HIE_COMMUNITY;
            case "BIOMETRICID": return FhirUri.IDENTIFIER_SYSTEM_CERNER_BIOMETRIC;
            case "NTKCRDNBR": return FhirUri.IDENTIFIER_SYSTEM_CERNER_CARD_NUMBER;
            default: return null;
        }
    }

}