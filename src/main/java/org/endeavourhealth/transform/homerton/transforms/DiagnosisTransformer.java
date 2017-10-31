package org.endeavourhealth.transform.homerton.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.endeavourhealth.transform.homerton.schema.Diagnosis;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DiagnosisTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(String version,
                                 Diagnosis parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }

    public static void createDiagnosis(Diagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode) throws Exception {
        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        Identifier patientIdentifier[] = {new Identifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID).setValue(StringUtils.deleteWhitespace(parser.getCNN()))};
        String uniqueId = "PatIdTypeCode=CNN-PatIdValue=" + parser.getCNN();
        ResourceId patientResourceId = resolvePatientResource(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, uniqueId, parser.getCurrentState(), null, fhirResourceFiler, parser.getCNN(), null, null,null, null, null, null, null, patientIdentifier, null, null, null);
        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare
        // Encounter
        //ResourceId encounterResourceId = resolveEncounterResource(parser.getCurrentState(), null,  parser.getEncounterId().toString(), fhirResourceFiler, patientResourceId, null, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(),parser.getDiagnosisDate());

        ResourceId encounterResourceId = getEncounterResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, parser.getEncounterId().toString());

            //Identifiers
            //Identifier encounterIdentifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

            //createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(), parser.getDiagnosisDate(), encounterIdentifiers, Encounter.EncounterClass.OTHER);
        }

        // this Diagnosis resource id
        //ResourceId diagnosisResourceId = getDiagnosisResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, parser.getCNN(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

        Condition.ConditionVerificationStatus cvs;
        if (parser.getActiveIndicator()) {
            cvs = Condition.ConditionVerificationStatus.CONFIRMED;
        } else {
            cvs = Condition.ConditionVerificationStatus.ENTEREDINERROR;
        }

        //CodeableConcept diagnosisCode = new CodeableConcept();
        //diagnosisCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());
        //CodeableConcept diagnosisCode = CodeableConceptHelper.createCodeableConcept(FhirUri.CODE_SYSTEM_SNOMED_CT, parser.getDiagnosis(), parser.getDiagnosisCode());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

        Condition fhirCondition = new Condition();
        //createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers, cvs);

        // save resource
        LOG.debug("Save Condition(PatId=" + parser.getCNN() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
    }


}
