package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPALI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPALITransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPALITransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                try {
                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    createPatientAlias((PPALI) parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    public static void createPatientAlias(PPALI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell aliasIdCell = parser.getMillenniumPersonAliasId();

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {

            String personIdStr = csvHelper.getInternalId(PPALIPreTransformer.PPALI_ID_TO_PERSON_ID, aliasIdCell.getString());
            if (!Strings.isNullOrEmpty(personIdStr)) {

                PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(Long.valueOf(personIdStr), csvHelper);
                if (patientBuilder != null) {
                    IdentifierBuilder.removeExistingIdentifierById(patientBuilder, aliasIdCell.getString());

                    csvHelper.getPatientCache().returnPatientBuilder(Long.valueOf(personIdStr), patientBuilder);
                }
            }
            return;
        }

        //if the alias is empty, there's nothing to add
        CsvCell aliasCell = parser.getAlias();
        if (aliasCell.isEmpty()) {
            return;
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //we always fully re-create the Identifier on the patient so just remove any previous instance
        IdentifierBuilder.removeExistingIdentifierById(patientBuilder, aliasIdCell.getString());

        //work out the system for the alias
        CsvCell aliasTypeCodeCell = parser.getAliasTypeCode();
        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.ALIAS_TYPE, aliasTypeCodeCell);

        String aliasDesc = cernerCodeValueRef.getCodeMeaningTxt();
        String aliasSystem = convertAliasCode(aliasDesc);

        if (aliasSystem == null) {
            TransformWarnings.log(LOG, parser, "Unknown alias system for {}", aliasDesc);
            aliasSystem = "UNKNWON";
        }

        //both the PPATI transform and PPALI transformers create Identifiers for the patient, although our file
        //has more information on them, so remove any existing Identifier for the same system that DOES NOT
        //have an ID (i.e. it didn't come from the PPALI file)
        List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(patientBuilder, aliasSystem);
        for (Identifier identifier : identifiers) {
            if (!identifier.hasId()) {
                patientBuilder.removeIdentifier(identifier);
            }
        }

        //if the alias record is an NHS number, then it's an official use. Secondary otherwise.
        Identifier.IdentifierUse use = null;
        if (aliasSystem.equalsIgnoreCase(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER)) {
            use = Identifier.IdentifierUse.OFFICIAL;

        } else {
            use = Identifier.IdentifierUse.SECONDARY;
        }

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.setId(aliasIdCell.getString(), aliasCell);
        identifierBuilder.setUse(use);
        identifierBuilder.setValue(aliasCell.getString(), aliasCell);
        identifierBuilder.setSystem(aliasSystem, aliasTypeCodeCell);

        CsvCell startDateCell = parser.getBeginEffectiveDate();
        if (!startDateCell.isEmpty()) {
            Date d = BartsCsvHelper.parseDate(startDateCell);
            identifierBuilder.setStartDate(d, startDateCell);
        }

        CsvCell endDateCell = parser.getEndEffectiveDate();
        //use this function to test the endDate cell, since it will have the Cerner end of time content
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {
            Date d = BartsCsvHelper.parseDate(endDateCell);
            identifierBuilder.setEndDate(d, endDateCell);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the PP... files
        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }


    private static String convertAliasCode(String statusCode) {
        switch (statusCode) {
            case "INTPERSID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNAL_PERSON;
            case "CMRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_COMMUNITY_MEDICAL_RECORD;
            case "DONORID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DONOR_ID;
            case "DNRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DONOR_NUMBER;
            case "DRLIC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DRIVING_LICENSE_NUMBER;
            case "FILLER ORDER":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_FILLER_ORDER;
            case "HISTCMRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_CMRN;
            case "HISTMRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HISTORICAL_MRN;
            case "HNASYSID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HNA_PAT_SYS_ID;
            case "MRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID;
            case "NHIN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "NMDP":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP;
            case "PASSPORT":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PASSPORT_NUMBER;
            case "PATID_V4":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_V400_OCF_PATIENT;
            case "PERSON NAME":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PERSON_NAME;
            case "PLACER ORDER":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PLACER_ORDER;
            case "PRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_PERSONNEL_ID;
            case "SSN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "SHIN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "UNOS":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_UNOS;
            case "HNAPERSONID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HNA_PERSON_ID;
            case "MILITARYID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_MILITARY_ID;
            case "OTHER":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OTHER_PERSON_ID;
            case "REF_MRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_REFERRING_MRN;
            case "OUTREACH":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OUTREACH_PERSON;
            case "PBSID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PBS_PATIENT;
            case "ACCOUNT":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ACCOUNT_NUMBER;
            case "MESSAGING":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_SECURE_MESSAGING;
            case "HICR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HIC_RECIPIENT;
            case "INTLD":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_INTERNATIONAL_DONOR;
            case "NMDPD":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP_DONOR;
            case "NMDPR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NMDP_RECIPIENT;
            case "OPOD":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OPO_DONOR;
            case "OPOR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_OPO_RECIPIENT;
            case "PXR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PX_ID;
            case "UNOSD":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_UNOS_DONOR;
            case "Cephalosporins":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CEPHALOSPORINS;
            case "Penicillins":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PENICILLINS;
            case "PASSWORD":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PASSWORD;
            case "VERSION":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_VERSION;
            case "MEMBERNBR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_MEMBER_NUMBER;
            case "Cortisol level":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "ZZZZCortisol level":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "RNJ R Gyne Secs GynaeObs Secs":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_RNJ;
            case "RNJ R Gyne SMgr GynaeService Mgr":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL_4HR;
            case "RNJ R NEUR ARK  Kelso":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CORTISOL_LEVEL;
            case "PERSONMEDIA":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PERSON_MEDIA_ALIAS;
            case "INSC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_NATIONAL_PATIENT_ID;
            case "MPI":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_HIE_COMMUNITY;
            case "BIOMETRICID":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_BIOMETRIC;
            case "NTKCRDNBR":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CARD_NUMBER;
            default:
                return null;
        }
    }

}