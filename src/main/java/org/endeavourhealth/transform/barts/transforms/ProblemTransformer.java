package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.transform.barts.schema.Problem;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.CsvCurrentState;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

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

        // Organisation - Since EpisodeOfCare record is not established no need for Organization either
        // Patient
        ResourceId patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), null, null, null, null, null, null, null);
        // EpisodeOfCare - Problem record cannot be linked to an EpisodeOfCare
        // Encounter - Problem record cannot be linked to an Encounter
        // this Problem resource id
        ResourceId problemResourceId = resolveProblemResourceId(primaryOrgOdsCode, fhirResourceFiler, parser.getLocalPatientId(), parser.getOnsetDateAsString(), parser.getProblemCode());

        Condition fhirCondition = new Condition();

        // Turn problem_id into Resource id
        fhirCondition.setId(problemResourceId.getResourceId().toString());

        fhirCondition.addIdentifier().setSystem("http://cerner.com/fhir/problem-id").setValue(parser.getProblemId().toString());

        // set patient reference
        fhirCondition.setPatient(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

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
