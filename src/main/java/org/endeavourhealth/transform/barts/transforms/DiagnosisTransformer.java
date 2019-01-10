package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.Diagnosis;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DiagnosisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    createDiagnosis((Diagnosis)parser, fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createDiagnosis(Diagnosis parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) {
        //TODO
    }

    //OLD 2.1 transform code is below//////////////////////////////////////////////////////////

    /*public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    createDiagnosis((Diagnosis)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createDiagnosis(Diagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()))};
        ResourceId patientResourceId = resolvePatientResource(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, null, parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null,null, null, null, organisationResourceId, null, patientIdentifier, null, null, null);

        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare
        // Encounter
        //ResourceId encounterResourceId = resolveEncounterResource(parser.getCurrentState(), null,  parser.getEncounterId().toString(), fhirResourceFiler, patientResourceId, null, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(),parser.getDiagnosisDate());

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        }

        //Identifiers
        Identifier encounterIdentifiers[] = {new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID).setValue(parser.getFINNbr())};

        createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(), parser.getDiagnosisDate(), encounterIdentifiers, Encounter.EncounterClass.OTHER);

        // this Diagnosis resource id
        ResourceId diagnosisResourceId = getDiagnosisResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocalPatientId(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

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

        Extension[] ex = {ExtensionConverter.createStringExtension(FhirExtensionUri.RESOURCE_CONTEXT , "clinical coding")};

        Condition fhirCondition = new Condition();
        createDiagnosis(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers, cvs, null, ex);

        // save resource
        if (parser.isActive()) {
            LOG.debug("Save Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
        } else {
            LOG.debug("Delete Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), new ConditionBuilder(fhirCondition));
        }

    }*/

}
