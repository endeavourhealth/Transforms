package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.endeavourhealth.transform.homerton.schema.Diagnosis;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DiagnosisTransformer extends BasisTransformer {
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
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        //ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null,null, null, null, null, null);
        // EpisodeOfCare - Diagnosis record cannot be linked to an EpisodeOfCare
        // Encounter
        //ResourceId encounterResourceId = resolveEncounterResource(parser.getCurrentState(), null,  parser.getEncounterId().toString(), fhirResourceFiler, patientResourceId, null, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(),parser.getDiagnosisDate());

        /*
        ResourceId encounterResourceId = getEncounterResourceId(parser.getEncounterId().toString());
        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(parser.getEncounterId().toString());

            //Identifiers
            Identifier encounterIdentifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

            createEncounter(parser.getCurrentState(),  fhirResourceFiler, patientResourceId, null,  encounterResourceId, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(), parser.getDiagnosisDate(), encounterIdentifiers, Encounter.EncounterClass.OTHER);
        }
        */

        // this Diagnosis resource id
        //ResourceId diagnosisResourceId = getDiagnosisResourceId(parser.getLocalPatientId(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());

        Condition fhirCondition = new Condition();

        CodeableConcept diagnosisCode = new CodeableConcept();
        //diagnosisCode.addCoding().setSystem(getCodeSystemName(HomertonCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getDiagnosis()).setCode(parser.getDiagnosisCode());

        //Identifiers
        //Identifier identifiers[] = {new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_DIAGNOSIS_ID).setValue(parser.getDiagnosisId().toString()), new Identifier().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_FIN_NO).setValue(parser.getFINNbr())};

        //createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), identifiers);

        // save resource
        LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));
        //savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);

    }

    public static void createDiagnosisResource(Condition fhirCondition, ResourceId diagnosisResourceId, ResourceId encounterResourceId, ResourceId patientResourceId, Date dateRecorded, DateTimeType onsetDate, CodeableConcept diagnosisCode, String notes, Identifier identifiers[] ) throws Exception {
        fhirCondition.setId(diagnosisResourceId.getResourceId().toString());

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirCondition.addIdentifier(identifiers[i]);
            }
        }

        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Encounter
        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // Date recorded
        fhirCondition.setDateRecorded(dateRecorded);

        fhirCondition.setOnset(onsetDate);

        // set code to coded problem - field 28
        fhirCondition.setCode(diagnosisCode);

        // set category to 'diagnosis'
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding().setSystem(HomertonCsvToFhirTransformer.CODE_SYSTEM_CONDITION_CATEGORY).setCode("diagnosis");
        fhirCondition.setCategory(cc);

        // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
        //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }


    }
}
