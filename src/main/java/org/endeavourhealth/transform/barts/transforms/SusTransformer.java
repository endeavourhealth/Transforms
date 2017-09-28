package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceIdHelper;
import org.endeavourhealth.transform.barts.schema.Sus;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class SusTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusTransformer.class);
    private static int entryCount = 0;

    public static void transform(String version,
                                 Sus parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        entryCount = 0;
        while (parser.nextRecord()) {
            try {
                entryCount++;
                if (parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 190) {
                    mapInpatient(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    if (parser.getCDSRecordType() == 20) {
                        mapOutpatient(parser, fhirResourceFiler, csvHelper, version);
                    } else {
                        if (parser.getCDSRecordType() == 10) {
                            mapAEpatient(parser, fhirResourceFiler, csvHelper, version);
                        }
                    }
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

    }

    // Data line is of type Inpatient
    public static void mapInpatient(Sus parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID) throws Exception {

        CodeableConcept cc = null;
        Date d = null;
        LOG.debug("Mapping IP entry (" + entryCount + ")");

        // ************************************
        // Patient
        // ************************************
        String uniqueId = "PIdAssAuth="+primaryOrgHL7OrgOID+"-PatIdValue="+parser.getLocalPatientId();
        ResourceId patientResourceId = ResourceIdHelper.getResourceId("B", "Patient", uniqueId);
        if (patientResourceId == null) {
            patientResourceId = new ResourceId();
            patientResourceId.setScopeId("B");
            patientResourceId.setResourceType("Patient");
            patientResourceId.setUniqueId(uniqueId);
            patientResourceId.setResourceId(UUID.randomUUID());

            LOG.trace("Create new Patient:" + patientResourceId.getUniqueId() + " resource:" + patientResourceId.getResourceId());

            ResourceIdHelper.saveResourceId(patientResourceId);

            Patient fhirPatient = createPatientResource(parser);
            fhirPatient.setId(patientResourceId.getResourceId().toString());
            LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirPatient));
            //fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirPatient);
        }

        // ************************************
        // Map Diagnosis information here
        // ************************************
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            Condition fhirCondition = new Condition();

            // Turn key into Resource id
            uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-CDSIdValue=" + parser.getCDSUniqueID() + "-DiagnosisIdValue=" + parser.getICDPrimaryDiagnosis();
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

            fhirCondition.addIdentifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID());

            // set patient reference
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
            d = parser.getAdmissionDateTime();
            fhirCondition.setDateRecorded(d);

            // set code to coded problem - field 28
            cc = new CodeableConcept();
            cc.addCoding().setSystem("http://hl7.org/fhir/ValueSet/icd-10").setCode(parser.getICDPrimaryDiagnosis());
            fhirCondition.setCode(cc);

            // set category to 'diagnosis'
            cc = new CodeableConcept();
            cc.addCoding().setSystem("http://hl7.org/fhir/condition-category").setCode("diagnosis");
            fhirCondition.setCategory(cc);

            // set verificationStatus - to field 8. Confirmed if value is 'Confirmed' otherwise ????
            // TODO check this
            //fhirCondition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);

            // save resource
            LOG.debug("Save Condition:" + FhirSerializationHelper.serializeResource(fhirCondition));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            // TODO - add secondary piagnoses

        } else {
            LOG.debug("No primary diagnosis present");
        }



        // ************************************
        // Map Procedure information here
        // ************************************
        if (parser.getOPCSPrimaryProcedureCode().length() > 0){
            Procedure fhirProcedure = new Procedure ();

            // save resource
            //fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId(), fhirCondition);
            LOG.debug("Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
        } else {
            LOG.debug("No primary procedure present");
        }

    }

    /*
     Data line is of type Outpatient
      */
    public static void mapOutpatient(Sus parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version) throws Exception {

        CodeableConcept cc = null;
        Date d = null;
        LOG.debug("Mapping OP entry (" + entryCount + ")");


    }

    // Data line is of type A&E
    public static void mapAEpatient(Sus parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     EmisCsvHelper csvHelper,
                                     String version) throws Exception {

        CodeableConcept cc = null;
        Date d = null;
        LOG.debug("Mapping AE entry (" + entryCount + ")");

    }


    /*
    Create a enw FHIR Patient Reshource
     */
    public static Patient createPatientResource(Sus parser) {
        Patient patient = new Patient();

        HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, parser.getPatientTitle(), parser.getPatientForename(), "", parser.getPatientSurname());
        patient.addName(name);

        if (parser.getAddressType().compareTo("02") == 0) {
            Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
            patient.addAddress(fhirAddress);
        }

        // Gender
       // Enumerations.AdministrativeGender gender = mapper.getCodeMapper().mapSex(sourcePid.getSex());
        //if (gender != null)
          //  patient.setGender(gender);


        // TODO
        /*
        *setDateOfBirth(source.getPidSegment(), target);
        *setSex(source.getPidSegment(), target);
        *addIdentifiers(source, target);-----NHS No ?????
        *addEthnicity(source.getPidSegment(), target);
        *addMaritalStatus(source.getPidSegment(), target);
        setPrimaryCareProvider(target);
        setManagingOrganization(source, target);
    */

        return patient;
    }

}
