package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceIdHelper;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class ProblemTransformer {
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
                createConditionResource(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createConditionResource(Problem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        Condition fhirCondition = new Condition();

        // Turn problem_id into Resource id
        String uniqueId = "ParentOdsCode="+primaryOrgOdsCode+"-ProblemIdValue="+parser.getProblemId().toString();
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

        fhirCondition.addIdentifier().setSystem("http://cerner.com/fhir/problem-id").setValue(parser.getProblemId().toString());

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
        }
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // Set Asserter - to field 32
        /*
        String names[] = parser.getUpdatedBy().split(",");
        uniqueId = "Surname="+names[0].trim()+"-Forename="+names[1].trim();
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
        d = parser.getUpdateDateTime();
        fhirCondition.setDateRecorded(d);

        // set code to coded problem - field 28
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getProblemCode());
        fhirCondition.setCode(cc);

        // set category to 'complaint'
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("complaint");
        fhirCondition.setCategory(cc);

        // set clinicalStatus - ???
        // in field 14 have so far seen 'Active', 'Resolved'
        // TODO

        // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
        // TODO

        //  set severity to field 16 + 17
        /*
        String severity = parser.getSeverity();
        if (severity != null) {
            cc = new CodeableConcept();
            //TODO set severity - should field severity_class be used ?
            //cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("complaint");
            fhirCondition.setSeverity(cc);
        } */

        // set onset to field  to field 10 + 11
        //d = parser.getOnsetDate();
        //Type type = new Type();
        //fhirCondition.setOnset().set.setDateRecorded(d);

        // set notes
        fhirCondition.setNotes(parser.getAnnotatedDisp());

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));

    }

}
