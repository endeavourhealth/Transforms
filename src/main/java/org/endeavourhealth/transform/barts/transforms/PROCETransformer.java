package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PROCETransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);


    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createProcedure((PROCE)parser, fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createProcedure(PROCE parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        UUID encounterUuid = csvHelper.findEncounterResourceIdFromEncounterId(encounterIdCell);
        UUID patientUuid = csvHelper.findPatientIdFromEncounterId(encounterIdCell);
        if (patientUuid == null) {
            LOG.warn("Skipping Procedure " + parser.getProcedureID().getString() + " due to missing encounter");
            return;
        }

        // this Procedure resource id
        CsvCell procedureIdCell = parser.getProcedureID();
        ResourceId procedureResourceId = getOrCreateProcedureResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, procedureIdCell);

        // create the FHIR Procedure
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        procedureBuilder.setId(procedureResourceId.getResourceId().toString(), procedureIdCell);

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, patientUuid.toString());
        procedureBuilder.setPatient(patientReference);


        if (!procedureIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_PROCEDURE_ID);
            identifierBuilder.setValue(procedureIdCell.getString(), procedureIdCell);
        }

        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            LOG.debug("Delete Procedure (" + procedureBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
            deletePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
            return;
        }

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();
        if (!procedureDateTimeCell.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(procedureDateTimeCell.getDate());
            procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterUuid.toString());
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!encounterSliceIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell nomenclatureIdCell = parser.getNomenclatureID();
        if (!nomenclatureIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirCodeUri.CODE_SYSTEM_CERNER_NOMENCLATURE_ID);
            identifierBuilder.setValue(nomenclatureIdCell.getString(), nomenclatureIdCell);
        }

        CsvCell personnelIdCell = parser.getPersonnelID();
        if (!personnelIdCell.isEmpty()) {
            ResourceId practitionerResourceId = getPractitionerResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, personnelIdCell);
            Reference practitionerReference = csvHelper.createPractitionerReference(practitionerResourceId.getResourceId().toString());
            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
        }

        // Procedure is coded either Snomed or OPCS4
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, ProcedureBuilder.TAG_CODEABLE_CONCEPT_CODE);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                String term = TerminologyService.lookupSnomedFromConceptId(conceptCode).getTerm();

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
            }

        } else {
            //LOG.warn("Unable to create codeableConcept for Procedure ID: "+procedureIdCell);
            return;
        }

        // Procedure type (category) is a Cerner Millenium code so lookup
        CsvCell procedureTypeCodeCell = parser.getProcedureTypeCode();
        BartsCodeableConceptHelper.applyCodeDisplayTxt(procedureTypeCodeCell, CernerCodeValueRef.PROCEDURE_TYPE, procedureBuilder, ProcedureBuilder.TAG_CODEABLE_CONCEPT_CATEGORY, csvHelper);

        // save resource
        LOG.debug("Save Procedure (" + procedureBuilder.getResourceId() + "):" + FhirSerializationHelper.serializeResource(procedureBuilder.getResource()));
        savePatientResource(fhirResourceFiler, parser.getCurrentState(), procedureBuilder);
    }

}
