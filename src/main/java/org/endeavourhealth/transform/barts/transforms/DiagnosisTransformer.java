package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.transform.barts.schema.Diagnosis;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DiagnosisTransformer extends BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisTransformer.class);

    public static void transform(String version,
                                 Diagnosis parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createDiagnosis(Diagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler);
        // Patient
        ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null,null, null, null, null, null);
        // EpisodeOfCare
        ResourceId episodeOfCareResourceId = resolveEpisodeResource(parser.getCurrentState(), primaryOrgHL7OrgOID, null, parser.getLocalPatientId(), parser.getEncounterId().toString(), parser.getFINNbr(), fhirResourceFiler, patientResourceId, organisationResourceId, parser.getDiagnosisDate(), EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
        // Encounter
        ResourceId encounterResourceId = resolveEncounterResource(parser.getCurrentState(), primaryOrgHL7OrgOID, null, parser.getLocalPatientId(), parser.getEncounterId().toString(), fhirResourceFiler, patientResourceId, episodeOfCareResourceId, Encounter.EncounterState.FINISHED, parser.getDiagnosisDate(),parser.getDiagnosisDate());
        // this Diagnosis resource id
        ResourceId diagnosisResourceId = getDiagnosisResourceId(primaryOrgOdsCode, fhirResourceFiler, parser.getLocalPatientId(), parser.getDiagnosisDateAsString(), parser.getDiagnosisCode());


        Condition fhirCondition = new Condition();

        CodeableConcept diagnosisCode = new CodeableConcept();
        diagnosisCode.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getDiagnosisCode());

        //Identifier identifiers[] = {new Identifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID())};

        createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getUpdateDateTime(), new DateTimeType(parser.getDiagnosisDate()), diagnosisCode, parser.getSecondaryDescription(), null);

        // save resource
        LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);

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
        cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("diagnosis");
        fhirCondition.setCategory(cc);

        // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
        // TODO check this
        //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }


    }
}
