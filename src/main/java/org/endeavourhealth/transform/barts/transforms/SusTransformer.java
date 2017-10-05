package org.endeavourhealth.transform.barts.transforms;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceIdHelper;
import org.endeavourhealth.transform.barts.schema.Sus;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
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
                // CDS V6-2 Type 010 - Accident and Emergency CDS
                // CDS V6-2-1 Type 011 - Emergency Care CDS (this is a future version Barts will eventually move to)
                // CDS V6-2 Type 020 - Outpatient CDS
                // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
                // CDS V6-2 Type 150 - Admitted Patient Care - Other Birth Event CDS
                // CDS V6-2 Type 160 - Admitted Patient Care - Other Delivery Event CDS
                // CDS V6-2 Type 180 - Admitted Patient Care - Unfinished Birth Episode CDS
                // CDS V6-2 Type 190 - Admitted Patient Care - Unfinished General Episode CDS
                // CDS V6-2 Type 200 - Admitted Patient Care - Unfinished Delivery Episode CDS
                if (parser.getCDSRecordType() == 10 ||
                        parser.getCDSRecordType() == 20 ||
                        parser.getCDSRecordType() == 120 ||
                        parser.getCDSRecordType() == 130 ||
                        parser.getCDSRecordType() == 140 ||
                        parser.getCDSRecordType() == 160 ||
                        parser.getCDSRecordType() == 180 ||
                        parser.getCDSRecordType() == 190 ||
                        parser.getCDSRecordType() == 200) {
                    mapFileEntry(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void mapFileEntry(Sus parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID) throws Exception {

        ResourceId patientResourceId = null;
        ResourceId encounterResourceId = null;

        // Map diagnosis codes ?
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            // Patient
            patientResourceId = resolvePatientResource(primaryOrgHL7OrgOID, parser, fhirResourceFiler);
            // Encounter
            encounterResourceId = resolveEncounterResource(primaryOrgHL7OrgOID, parser, fhirResourceFiler);
            // Diagnosis
            mapDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId);
        } else {
            LOG.debug("No primary diagnosis present");
        }

        // Map procedure codes ?
        if (parser.getOPCSPrimaryProcedureCode().length() > 0) {
            // Patient
            if (patientResourceId == null) {
                patientResourceId = resolvePatientResource(primaryOrgHL7OrgOID, parser, fhirResourceFiler);
            }
            // Encounter
            if (encounterResourceId == null) {
                encounterResourceId = resolveEncounterResource(primaryOrgHL7OrgOID, parser, fhirResourceFiler);
            }
            // Diagnosis
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId);
        } else {
            LOG.debug("No primary procedure present");
        }

    }


    /*
    Data line is of type Inpatient
    */
    public static void mapDiagnosis(Sus parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId) throws Exception {

        CodeableConcept cc = null;
        Date d = null;
        String uniqueId;
        LOG.debug("Mapping Diagnosis from file entry (" + entryCount + ")");

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

        // Encounter
        if (encounterResourceId != null) {
            fhirCondition.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

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

        // set code to coded problem
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
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        } else {
            LOG.debug("Save primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        }

        // secondary piagnoses
        for (int i = 0; i < parser.getICDSecondaryDiagnosisCount(); i++) {
            // Turn key into Resource id
            uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-CDSIdValue=" + parser.getCDSUniqueID() + "-DiagnosisIdValue=" + parser.getICDSecondaryDiagnosis(i);
            resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
            if (resourceId == null) {
                resourceId = new ResourceId();
                resourceId.setScopeId("B");
                resourceId.setResourceType("Condition");
                resourceId.setUniqueId(uniqueId);
                resourceId.setResourceId(UUID.randomUUID());
                ResourceIdHelper.saveResourceId(resourceId);
            }
            fhirCondition.setId(resourceId.getResourceId().toString());

            // set code to coded problem
            cc = new CodeableConcept();
            cc.addCoding().setSystem("http://hl7.org/fhir/ValueSet/icd-10").setCode(parser.getICDSecondaryDiagnosis(i));
            fhirCondition.setCode(cc);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            } else {
                LOG.debug("Save primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            }

        }
    }

    /*
Data line is of type Inpatient
*/
    public static void mapProcedure(Sus parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId) throws Exception {

        CodeableConcept cc = null;
        Date d = null;
        String uniqueId;
        LOG.debug("Mapping Procedure from file entry (" + entryCount + ")");

        Procedure fhirProcedure = new Procedure ();

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
        fhirProcedure.setId(resourceId.getResourceId().toString());

        fhirProcedure.addIdentifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID());

        // set patient reference
        fhirProcedure.setSubject(ReferenceHelper.createReference(ResourceType.Patient, patientResourceId.getResourceId().toString()));

        // status
        // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
        // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
        // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
        if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
            fhirProcedure.setStatus(Procedure.ProcedureStatus.COMPLETED);
        } else {
            fhirProcedure.setStatus(Procedure.ProcedureStatus.INPROGRESS);
        }

        // Code
        cc = new CodeableConcept();
        cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getOPCSPrimaryProcedureCode());
        fhirProcedure.setCode(cc);

        // Performed date/time
        Timing t = new Timing().addEvent(parser.getOPCSPrimaryProcedureDate());
        fhirProcedure.setPerformed(t);

        // Encounter
        if (encounterResourceId != null) {
            fhirProcedure.setEncounter(ReferenceHelper.createReference(ResourceType.Encounter, encounterResourceId.getResourceId().toString()));
        }

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Save primary Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
        } else {
            LOG.debug("Save primary Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
        }

        // secondary procedures
        for (int i = 0; i < parser.getOPCSecondaryProcedureCodeCount(); i++) {
            // Turn key into Resource id
            uniqueId = "ParentOdsCode=" + primaryOrgOdsCode + "-CDSIdValue=" + parser.getCDSUniqueID() + "-DiagnosisIdValue=" + parser.getOPCSecondaryProcedureCode(i);
            resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
            if (resourceId == null) {
                resourceId = new ResourceId();
                resourceId.setScopeId("B");
                resourceId.setResourceType("Condition");
                resourceId.setUniqueId(uniqueId);
                resourceId.setResourceId(UUID.randomUUID());
                ResourceIdHelper.saveResourceId(resourceId);
            }
            fhirProcedure.setId(resourceId.getResourceId().toString());

            // Code
            cc = new CodeableConcept();
            cc.addCoding().setSystem("http://snomed.info/sct").setCode(parser.getOPCSecondaryProcedureCode(i));
            fhirProcedure.setCode(cc);

            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete secondary Procedure (" + i + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
            } else {
                LOG.debug("Save secondary Procedure (" + i + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
            }
        }

    }

    /*
        Encounter resources are not maintained by this feed. They are only created if missing. Encounter status etc. is maintained by the HL7 feedd
     */
    public static ResourceId resolveEncounterResource(String primaryOrgHL7OrgOID, Sus parser, FhirResourceFiler fhirResourceFiler) throws Exception {
        ResourceId resourceId = null;
        TailsRecord tr = TailsPreTransformer.getTailsRecord(parser.getCDSUniqueID());
        if (tr != null) {
            String uniqueId = "PIdAssAuth=" + primaryOrgHL7OrgOID+"-PatIdValue="+parser.getLocalPatientId()+"-EpIdTypeCode=VISITID-EpIdValue="+tr.getEncounterId();
            resourceId = ResourceIdHelper.getResourceId("B", "Condition", uniqueId);
            if (resourceId == null) {
                resourceId = new ResourceId();
                resourceId.setScopeId("B");
                resourceId.setResourceType("Encounter");
                resourceId.setUniqueId(uniqueId);
                resourceId.setResourceId(UUID.randomUUID());

                LOG.trace("Create new Encounter:" + resourceId.getUniqueId() + " resource:" + resourceId.getResourceId());
                ResourceIdHelper.saveResourceId(resourceId);

                Encounter fhirEncounter = new Encounter();
                fhirEncounter.setId(resourceId.getResourceId().toString());

                if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
                    fhirEncounter.setStatus(Encounter.EncounterState.FINISHED);
                } else {
                    fhirEncounter.setStatus(Encounter.EncounterState.INPROGRESS);
                }

                LOG.debug("Save Encounter:" + FhirSerializationHelper.serializeResource(fhirEncounter));
                fhirResourceFiler.savePatientResource(parser.getCurrentState(), fhirEncounter.getId().toString(), fhirEncounter);
            }
        }
        return resourceId;
    }

    public static ResourceId resolvePatientResource(String primaryOrgHL7OrgOID, Sus parser, FhirResourceFiler fhirResourceFiler) throws Exception {
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

            Patient fhirPatient = createPatientResource(parser, primaryOrgHL7OrgOID);
            fhirPatient.setId(patientResourceId.getResourceId().toString());

            LOG.debug("Save Patient:" + FhirSerializationHelper.serializeResource(fhirPatient));
            fhirResourceFiler.savePatientResource(parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirPatient);
        }
        return patientResourceId;
    }

    /*
    Create a enw FHIR Patient Reshource
     */
    public static Patient createPatientResource(Sus parser, String primaryOrgHL7OrgOID) {
        Patient patient = new Patient();

        Identifier patientIdentifier = new Identifier()
                .setSystem("http://endeavourhealth.org/fhir/id/v2-local-patient-id/barts-mrn")
                .setValue(StringUtils.deleteWhitespace(parser.getLocalPatientId()));
        patient.addIdentifier(patientIdentifier);


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
