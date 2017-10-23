package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProblemTransformer extends BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemTransformer.class);

    public static void transform(String version,
                                 Problem parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createCondition(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createCondition(Problem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - Problem record cannot be linked to an EpisodeOfCare
        // Encounter - Problem record cannot be linked to an Encounter
        // this Problem resource id
        ResourceId problemResourceId = getProblemResourceId(parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());

        CodeableConcept problemCode = new CodeableConcept();
        problemCode.addCoding().setCode(parser.getProblemCode()).setSystem(getCodeSystemName(BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED)).setDisplay(parser.getProblem());

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_PROBLEM_ID).setValue(parser.getProblemId().toString())};

        DateTimeType onsetDate = new DateTimeType(parser.getOnsetDate());

        Condition fhirCondition = new Condition();
        createConditionResource(fhirCondition, problemResourceId, patientResourceId, null, parser.getUpdateDateTime(), problemCode, onsetDate, parser.getAnnotatedDisp(), identifiers);

        // save resource
        LOG.debug("Save Condition(PatId=" + parser.getLocalPatientId() + "):" + FhirSerializationHelper.serializeResource(fhirCondition));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
    }

    public static void createConditionResource(Condition fhirCondition, ResourceId problemResourceId, ResourceId patientResourceId, ResourceId encounterResourceId, Date dateRecorded, CodeableConcept problemCode, DateTimeType onsetDate, String notes, Identifier identifiers[]) throws Exception {
        fhirCondition.setId(problemResourceId.getResourceId().toString());

        fhirCondition.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROBLEM));

        if (identifiers != null) {
            for (int i = 0; i < identifiers.length; i++) {
                fhirCondition.addIdentifier(identifiers[i]);
            }
        }

        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }
        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Date recorded
        fhirCondition.setDateRecorded(dateRecorded);

        // set code to coded problem
        if (problemCode.getText() == null || problemCode.getText().length() == 0) {
            problemCode.setText(problemCode.getCoding().get(0).getDisplay());
        }
        fhirCondition.setCode(problemCode);

        // set category to 'complaint'
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding().setSystem(BartsCsvToFhirTransformer.CODE_SYSTEM_CONDITION_CATEGORY).setCode("complaint");
        fhirCondition.setCategory(cc);

        // set onset to field  to field 10 + 11
        fhirCondition.setOnset(onsetDate);

        // set notes
        if (notes != null) {
            fhirCondition.setNotes(notes);
        }

    }

}
