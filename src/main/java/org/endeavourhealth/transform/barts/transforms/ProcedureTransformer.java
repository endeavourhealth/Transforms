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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class ProcedureTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);
    public static final DateFormat resourceIdFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public static void transform(String version,
                                 org.endeavourhealth.transform.barts.schema.Procedure parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        // Skip header line
        parser.nextRecord();

        while (parser.nextRecord()) {
            try {
                createProcedureResource(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }


    public static void createProcedureResource(org.endeavourhealth.transform.barts.schema.Procedure parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {
        CodeableConcept cc = null;
        Date d = null;

        Procedure fhirProcedure = new Procedure();

        // Turn key into Resource id
        String resourceType = "Procedure";
        String createDateTime = resourceIdFormat.format(parser.getCreateDateTime());
        String uniqueId = "ParentOdsCode="+primaryOrgOdsCode+"-ProcedureCode="+parser.getProcedureCode()+"-CreateDateTime="+createDateTime;
        ResourceId resourceId = ResourceIdHelper.getResourceId("B", resourceType, uniqueId);
        if (resourceId == null) {
            resourceId = new ResourceId();
            resourceId.setScopeId("B");
            resourceId.setResourceType(resourceType);
            resourceId.setUniqueId(uniqueId);
            resourceId.setResourceId(UUID.randomUUID());
            ResourceIdHelper.saveResourceId(resourceId);
        }
        fhirProcedure.setId(resourceId.getResourceId().toString());

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
                    .setValue(parser.getLocalPatientId());
            fhirPatient.addIdentifier(patientIdentifier);

            if (parser.getNHSNo().length() > 0) {
                patientIdentifier = new Identifier()
                        .setSystem("http://fhir.nhs.net/Id/nhs-number")
                        .setValue(parser.getNHSNo());
                fhirPatient.addIdentifier(patientIdentifier);
            }

            fhirPatient.setBirthDate(parser.getDOB());

            LOG.debug("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirPatient);
        }
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // status
        fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);

        // Code
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getProcedureCode());
        fhirProcedure.setCode(cc);

        // Performed date/time
        Timing t = new Timing().addEvent(parser.getProcedureDateTime());
        fhirProcedure.setPerformed(t);

        // set notes
        fhirProcedure.addNotes(new Annotation().setText(parser.getProcedureText()));

        // save resource
        LOG.debug("Save Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirProcedure);

    }

}
