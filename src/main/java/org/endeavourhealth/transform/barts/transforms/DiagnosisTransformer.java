package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceIdHelper;
import org.endeavourhealth.transform.barts.schema.Diagnosis;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class DiagnosisTransformer {
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
                createDiagnosisResource(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createDiagnosisResource(Diagnosis parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        Condition fhirCondition = new Condition();

        // Turn key into Resource id
        String uniqueId = "ParentOdsCode="+primaryOrgOdsCode+"-ProblemIdValue="+parser.getDiagnosisId().toString();
        ResourceId resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId("B");
            resourceId.setResourceType("Condition");
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            ResourceIdHelper.saveResourceId(resourceId);
        }
        fhirCondition.setId(resourceId.getResourceId().toString());

        //fhirCondition.addIdentifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID());

        // set patient reference
        uniqueId = "PIdAssAuth="+primaryOrgHL7OrgOID+"-PatIdValue="+parser.getLocalPatientId();
        ResourceId patientResourceId = ResourceIdHelper.getResourceId("B", "Patient", uniqueId);
        if (patientResourceId == null) {
            patientResourceId = new ResourceId();
            patientResourceId.setScopeId("B");
            patientResourceId.setResourceType("Patient");
            patientResourceId.setUniqueId(uniqueId);
            patientResourceId.setResourceId(UUID.randomUUID());
            ResourceIdHelper.saveResourceId(patientResourceId);

            Patient fhirPatient = new Patient();
            fhirPatient.setId(patientResourceId.getResourceId().toString());

            Identifier patientIdentifier = new Identifier()
                    .setSystem("http://endeavourhealth.org/fhir/id/v2-local-patient-id/barts-mrn")
                    .setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()));
            fhirPatient.addIdentifier(patientIdentifier);

            LOG.debug("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirPatient);
        }
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Encounter ???
        // TODO Can we find encounter?

        // Asserter
            /*
            uniqueId = "ConsultantCode=" + parser.getConsultantCode();
            resourceId = ResourceIdHelper.getResourceId("B", "Practitioner", uniqueId);
            if (resourceId == null) {
                resourceId = new ResourceId();
                resourceId.setScopeId("B");
                resourceId.setResourceType("Practitioner");
                resourceId.setUniqueId(uniqueId);
                resourceId.setResourceId(UUID.randomUUID());
                ResourceIdHelper.saveResourceId(resourceId);
            }
            fhirCondition.setAsserter(ReferenceHelper.createReference(ResourceType.Practitioner, resourceId.getResourceId().toString()));
            */

        // Date recorded
        d = parser.getDiagnosisDate();
        fhirCondition.setDateRecorded(d);

        // set code to coded problem - field 28
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getDiagnosisCode());
        fhirCondition.setCode(cc);

        // set category to 'diagnosis'
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("diagnosis");
        fhirCondition.setCategory(cc);

        // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
        // TODO check this
        //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

        // set notes
        fhirCondition.setNotes(parser.getDiagnosis());

        // save resource
        LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);

    }

}
