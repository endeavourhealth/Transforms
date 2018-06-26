package org.endeavourhealth.transform.barts.transformsOld;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.BulkDiagnosis;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class BulkDiagnosisTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDiagnosisTransformer.class);

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createDiagnosis((BulkDiagnosis) parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createDiagnosis(BulkDiagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient - only create if missing
        CsvCell localPatientIdCell = parser.getLocalPatientId();
        String localPatientId = localPatientIdCell.getString();
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(localPatientId))};

        //TODO - fix this (if this transform is needed). Change to use normal ID mapping, rather than doing all mapping in the HL7 Receiver database
        if (true) {
            throw new RuntimeException("Code needs fixing");
        }
        ResourceId patientResourceId = null;
        /*ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, localPatientId);
        if (patientResourceId == null) {
            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, localPatientId, null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        }*/
        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare

        CsvCell diagnosisIdCell = parser.getDiagnosisId();
        String diagnosisId = diagnosisIdCell.getString().split("\\.")[0];

        // Encounter
        CsvCell encounterIdCell = parser.getEncounterId();
        String encounterId = encounterIdCell.getString().split("\\.")[0];

        CsvCell diagnosisDateCell = parser.getDiagnosisDate();

        CsvCell finNumberCell = parser.getFINNbr();
        String fnnNumber = finNumberCell.getString();

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);

            Identifier encounterIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID).setValue(diagnosisId), new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(fnnNumber)};
            Date diagnosisDate = diagnosisDateCell.getDate();

            createEncounter(parser.getCurrentState(), fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, diagnosisDate, diagnosisDate, encounterIdentifiers, Encounter.EncounterClass.OTHER);
        }

        // this Diagnosis resource id
        CsvCell diagnosisCodeCell = parser.getDiagnosisCode();
        ResourceId diagnosisResourceId = readDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, localPatientId, diagnosisDateCell.getString(), diagnosisCodeCell.getString());
        if (diagnosisResourceId == null) {
            diagnosisResourceId = getDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, localPatientId, diagnosisDateCell.getString(), diagnosisCodeCell.getString());

            Condition.ConditionVerificationStatus cvs;
            //LOG.debug("ActiveIndicator=" + parser.getActiveIndicator());
            CsvCell activeIndicator = parser.getActiveIndicator();
            if (activeIndicator.getIntAsBoolean()) {
                cvs = Condition.ConditionVerificationStatus.CONFIRMED;
            } else {
                cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
            }

            //CodeableConcept diagnosisCode = new CodeableConcept();
            //diagnosisCode.addCoding().setSystem(getCodeSystemName(FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());
            CsvCell diagnosisTermCell = parser.getDiagnosis();
            CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, diagnosisTermCell.getString(), diagnosisCodeCell.getString());

            //Identifiers
            Identifier identifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID).setValue(diagnosisId), new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(fnnNumber)};

            Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "clinical coding")};

            Date diagnosisDate = diagnosisDateCell.getDate();
            Date updatedDate = parser.getUpdateDateTime().getDate();
            String comments = parser.getSecondaryDescription().getString();

            Condition fhirCondition = new Condition();
            createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, updatedDate, new DateTimeType(diagnosisDate), diagnosisCode, comments, identifiers, cvs, null, ex);

            // save resource
            LOG.debug("Save Condition(PatId=" + localPatientId + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
        }

    }

    /*
     *
     */
    /*public static void createDiagnosis(BulkDiagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient - only create if missing
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};
        ResourceId patientResourceId = getPatientResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, primaryOrgHL7OrgOID, parser.getLocalPatientId());
        if (patientResourceId == null) {
            patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);
        }
        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare

        // Encounter
        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());

            Identifier encounterIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(parser.getFINNbr())};

            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(), parser.getDiagnosisDate(), encounterIdentifiers, Encounter.EncounterClass.OTHER);
        }

        // this Diagnosis resource id
        ResourceId diagnosisResourceId = readDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocalPatientId(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());
        if (diagnosisResourceId == null) {
            diagnosisResourceId = getDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocalPatientId(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

            Condition.ConditionVerificationStatus cvs;
            //LOG.debug("ActiveIndicator=" + parser.getActiveIndicator());
            if (parser.isActive()) {
                cvs = Condition.ConditionVerificationStatus.CONFIRMED;
            } else {
                cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
            }

            //CodeableConcept diagnosisCode = new CodeableConcept();
            //diagnosisCode.addCoding().setSystem(getCodeSystemName(FhirCodeUri.CODE_SYSTEM_CERNER_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());
            CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, parser.getDiagnosis(), parser.getDiagnosisCode());

            //Identifiers
            Identifier identifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(parser.getFINNbr())};

            Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT, "clinical coding")};

            Condition fhirCondition = new Condition();
            createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers, cvs, null, ex);

            // save resource
            LOG.debug("Save Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        }

    }*/

}
