package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.rdbms.hl7receiver.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.SusOutpatient;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SusOutpatientTransformer extends BasisTransformer{
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientTransformer.class);
    private static int entryCount = 0;

    public static void transform(String version,
                                 SusOutpatient parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        entryCount = 0;
        while (parser.nextRecord()) {
            try {
                entryCount++;
                // CDS V6-2 Type 010 - Accident and Emergency CDS
                // CDS V6-2 Type 020 - Outpatient CDS
                // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
                // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
                // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
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

    public static void mapFileEntry(SusOutpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID) throws Exception {

        ResourceId patientResourceId = null;
        ResourceId organisationResourceId = null;
        ResourceId episodeOfCareResourceId = null;
        ResourceId encounterResourceId = null;

        if ((parser.getICDPrimaryDiagnosis().length() > 0) || (parser.getOPCSPrimaryProcedureCode().length() > 0)) {
            // Organisation
            organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler);
            // Patient
            HumanName name = org.endeavourhealth.common.fhir.NameConverter.createHumanName(HumanName.NameUse.OFFICIAL, parser.getPatientTitle(), parser.getPatientForename(), "", parser.getPatientSurname());
            Address fhirAddress = null;
            if (parser.getAddressType().compareTo("02") == 0) {
                fhirAddress = AddressConverter.createAddress(Address.AddressUse.HOME, parser.getAddress1(), parser.getAddress2(), parser.getAddress3(), parser.getAddress4(), parser.getAddress5(), parser.getPostCode());
            }
            patientResourceId = resolvePatientResource(parser.getCurrentState(), primaryOrgHL7OrgOID, fhirResourceFiler, parser.getLocalPatientId(), parser.getNHSNo(), name, fhirAddress, convertSusGenderToFHIR(parser.getGender()), parser.getDOB(), organisationResourceId, null);
            // EpisodeOfCare
            episodeOfCareResourceId = resolveEpisodeResource(parser.getCurrentState(), primaryOrgHL7OrgOID, parser.getCDSUniqueID(), parser.getLocalPatientId(), null, null, fhirResourceFiler, patientResourceId, organisationResourceId, parser.getAppointmentDateTime(), EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
            // Encounter
            encounterResourceId = resolveEncounterResource(parser.getCurrentState(), primaryOrgHL7OrgOID, parser.getCDSUniqueID(), parser.getLocalPatientId(), null, fhirResourceFiler, patientResourceId, episodeOfCareResourceId, Encounter.EncounterState.FINISHED, parser.getAppointmentDateTime(), parser.getAppointmentDateTime());
        }

        // Map diagnosis codes ?
        if (parser.getICDPrimaryDiagnosis().length() > 0) {
            // Diagnosis
            mapDiagnosis(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId);
        }

        // Map procedure codes ?
        if (parser.getOPCSPrimaryProcedureCode().length() > 0) {
            // Procedure
            mapProcedure(parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID, patientResourceId, encounterResourceId);
        }

    }


    /*
    Data line is of type Inpatient
    */
    public static void mapDiagnosis(SusOutpatient parser,
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

        // Turn key into Resource id
        ResourceId diagnosisResourceId = getDiagnosisResourceIdFromCDSData(primaryOrgOdsCode, fhirResourceFiler, parser.getCDSUniqueID(), parser.getICDPrimaryDiagnosis());

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID())};

        CodeableConcept diagnosisCode = new CodeableConcept();
        diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDPrimaryDiagnosis(), BartsCsvToFhirTransformer.CODE_SYSTEM_ICD_10, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, true);

        Condition fhirCondition = new Condition();
        DiagnosisTransformer.createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getAppointmentDateTime(), new DateTimeType(parser.getAppointmentDateTime()), diagnosisCode, null, null);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Delete primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        } else {
            LOG.debug("Save primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
        }

        // secondary piagnoses ?
        for (int i = 0; i < parser.getICDSecondaryDiagnosisCount(); i++) {
            // Turn key into Resource id
            diagnosisResourceId = getDiagnosisResourceIdFromCDSData(primaryOrgOdsCode, fhirResourceFiler, parser.getCDSUniqueID(), parser.getICDSecondaryDiagnosis(i));

            diagnosisCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_DIAGNOSIS, parser.getICDSecondaryDiagnosis(i), BartsCsvToFhirTransformer.CODE_SYSTEM_ICD_10, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, true);

            fhirCondition = new Condition();
            DiagnosisTransformer.createDiagnosisResource(fhirCondition, diagnosisResourceId, encounterResourceId, patientResourceId, parser.getAppointmentDateTime(), new DateTimeType(parser.getAppointmentDateTime()), diagnosisCode, null, null);

            // save resource
            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            } else {
                LOG.debug("Save primary Condition resource:" + FhirSerializationHelper.serializeResource(fhirCondition));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), patientResourceId.getResourceId().toString(), fhirCondition);
            }

        }
    }

    /*
Data line is of type Inpatient
*/
    public static void mapProcedure(SusOutpatient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version,
                                    String primaryOrgOdsCode,
                                    String primaryOrgHL7OrgOID,
                                    ResourceId patientResourceId,
                                    ResourceId encounterResourceId) throws Exception {

        LOG.debug("Mapping Procedure from file entry (" + entryCount + ")");

        // Turn key into Resource id
        ResourceId resourceId = getProcedureResourceId(primaryOrgOdsCode, fhirResourceFiler, parser.getCDSUniqueID(), parser.getLocalPatientId(), null, parser.getOPCSPrimaryProcedureDateAsString(), parser.getOPCSPrimaryProcedureCode());

        //Identifiers
        Identifier identifiers[] = {new Identifier().setSystem("http://cerner.com/fhir/cds-unique-id").setValue(parser.getCDSUniqueID())};

        // status
        // CDS V6-2 Type 120 - Admitted Patient Care - Finished Birth Episode CDS
        // CDS V6-2 Type 130 - Admitted Patient Care - Finished General Episode CDS
        // CDS V6-2 Type 140 - Admitted Patient Care - Finished Delivery Episode CDS
        Procedure.ProcedureStatus procStatus;
        if (parser.getCDSRecordType() == 120 || parser.getCDSRecordType() == 130 || parser.getCDSRecordType() == 140) {
            procStatus = Procedure.ProcedureStatus.COMPLETED;
        } else {
            procStatus = Procedure.ProcedureStatus.INPROGRESS;
        }

        // Code
        CodeableConcept procedureCode = new CodeableConcept();
        procedureCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_PROCEDURE, parser.getOPCSPrimaryProcedureCode(), BartsCsvToFhirTransformer.CODE_SYSTEM_OPCS_4, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, true);

        Procedure fhirProcedure = new Procedure ();
        ProcedureTransformer.createProcedureResource(fhirProcedure, resourceId, encounterResourceId, patientResourceId, procStatus, procedureCode, parser.getOPCSPrimaryProcedureDate(), null, identifiers);

        // save resource
        if (parser.getCDSUpdateType() == 1) {
            LOG.debug("Save primary Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
        } else {
            LOG.debug("Save primary Procedure:" + FhirSerializationHelper.serializeResource(fhirProcedure));
            savePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
        }

        // secondary procedures
        LOG.debug("Secondary procedure count:" + parser.getOPCSecondaryProcedureCodeCount());
        for (int i = 0; i < parser.getOPCSecondaryProcedureCodeCount(); i++) {
            // Turn key into Resource id
            resourceId = getProcedureResourceId(primaryOrgOdsCode, fhirResourceFiler, parser.getCDSUniqueID(), parser.getLocalPatientId(), null, parser.getOPCSecondaryProcedureDateAsString(i), parser.getOPCSecondaryProcedureCode(i));

            // Code
            procedureCode = mapToCodeableConcept(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, BartsCsvToFhirTransformer.CODE_CONTEXT_PROCEDURE, parser.getOPCSecondaryProcedureCode(i), BartsCsvToFhirTransformer.CODE_SYSTEM_OPCS_4, BartsCsvToFhirTransformer.CODE_SYSTEM_SNOMED, true);

            fhirProcedure = new Procedure ();
            ProcedureTransformer.createProcedureResource(fhirProcedure, resourceId, encounterResourceId, patientResourceId, procStatus, procedureCode, parser.getOPCSecondaryProcedureDate(i), null, identifiers);

            if (parser.getCDSUpdateType() == 1) {
                LOG.debug("Delete secondary Procedure (" + i + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                deletePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
            } else {
                LOG.debug("Save secondary Procedure (" + i + "):" + FhirSerializationHelper.serializeResource(fhirProcedure));
                savePatientResource(fhirResourceFiler, parser.getCurrentState(), fhirProcedure.getId(), fhirProcedure);
            }
        }

    }


}
