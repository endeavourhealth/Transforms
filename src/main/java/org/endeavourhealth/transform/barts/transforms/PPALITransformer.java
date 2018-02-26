package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
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

        CsvCell milleniumPersonIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(milleniumPersonIdCell, csvHelper);

        if (patientBuilder == null) {
            LOG.warn("Skipping PPALI record for " + milleniumPersonIdCell.getString() + " as no MRN->Person mapping found");
            return;
        }

        //we always fully re-create the Identifier on the patient so just remove any previous instance
        CsvCell aliasIdCell = parser.getMillenniumPersonAliasId();
        IdentifierBuilder.removeExistingIdentifierById(patientBuilder, aliasIdCell.getString());

        //if this record is no longer active, just return out, since we've already removed the Identifier from the patient
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.setId(aliasIdCell.getString(), aliasCell);
        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilder.setValue(aliasCell.getString(), aliasCell);

        // Patient Alias (these are all secondary as MRN and NHS are added in PPATI
        CsvCell aliasTypeCodeCell = parser.getAliasTypeCode();
        if (!aliasTypeCodeCell.isEmpty() && aliasTypeCodeCell.getLong() > 0) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                    CernerCodeValueRef.ALIAS_TYPE,
                    aliasTypeCodeCell.getLong());

            String aliasSystem = convertAliasCode(cernerCodeValueRef.getCodeMeaningTxt());
            identifierBuilder.setSystem(aliasSystem, aliasTypeCodeCell);
        }

        CsvCell startDateCell = parser.getBeginEffectiveDate();
        if (!startDateCell.isEmpty()) {
            identifierBuilder.setStartDate(startDateCell.getDate(), startDateCell);
        }

        CsvCell endDateCell = parser.getEndEffectiveDate();
        if (!endDateCell.isEmpty()) {
            identifierBuilder.setEndDate(startDateCell.getDate(), startDateCell);
        }
    }


    private static String convertAliasCode(String statusCode) {
        switch (statusCode) {
            case "INTPERSID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON;
            case "CMRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_COMMUNITY_MEDICAL_RECORD;
            case "DONORID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DONOR_ID;
            case "DNRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DONOR_NUMBER;
            case "DRLIC": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DRIVING_LICENSE_NUMBER;
            case "FILLER ORDER": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_FILLER_ORDER;
            case "HISTCMRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_CMRN;
            case "HISTMRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_MRN;
            case "HNASYSID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HNA_PAT_SYS_ID;
            case "MRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID;
            case "NHIN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "NMDP": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP;
            case "PASSPORT": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PASSPORT_NUMBER;
            case "PATID_V4": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_V400_OCF_PATIENT;
            case "PERSON NAME": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PERSON_NAME;
            case "PLACER ORDER": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PLACER_ORDER;
            case "PRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID;
            case "SSN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "SHIN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "UNOS": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_UNOS;
            case "HNAPERSONID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HNA_PERSON_ID;
            case "MILITARYID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_MILITARY_ID;
            case "OTHER": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OTHER_PERSON_ID;
            case "REF_MRN": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_REFERRING_MRN;
            case "OUTREACH": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OUTREACH_PERSON;
            case "PBSID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PBS_PATIENT;
            case "ACCOUNT": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ACCOUNT_NUMBER;
            case "MESSAGING": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_SECURE_MESSAGING;
            case "HICR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HIC_RECIPIENT;
            case "INTLD": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNATIONAL_DONOR;
            case "NMDPD": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP_DONOR;
            case "NMDPR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP_RECIPIENT;
            case "OPOD": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OPO_DONOR;
            case "OPOR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OPO_RECIPIENT;
            case "PXR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PX_ID;
            case "UNOSD": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_UNOS_DONOR;
            case "Cephalosporins": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CEPHALOSPORINS;
            case "Penicillins": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PENICILLINS;
            case "PASSWORD": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PASSWORD;
            case "VERSION": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_VERSION;
            case "MEMBERNBR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_MEMBER_NUMBER;
            case "Cortisol level": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "ZZZZCortisol level": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "RNJ R Gyne Secs GynaeObs Secs": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_RNJ;
            case "RNJ R Gyne SMgr GynaeService Mgr": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "RNJ R NEUR ARK  Kelso": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "PERSONMEDIA": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PERSON_MEDIA_ALIAS;
            case "INSC": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NATIONAL_PATIENT_ID;
            case "MPI": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HIE_COMMUNITY;
            case "BIOMETRICID": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_BIOMETRIC;
            case "NTKCRDNBR": return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CARD_NUMBER;
            default: return null;
        }
    }

}