package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.barts.schema.ProcedurePojo;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PROCETransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);
    private static final String TWO_DECIMAL_PLACES = ".00";


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                try {
                    createProcedure((PROCE) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createProcedure(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // this Procedure resource id
        CsvCell procedureIdCell = parser.getProcedureID();

        //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the person ID
        //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            Procedure existingProcedure = (Procedure) csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureIdCell);
            if (existingProcedure != null) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder(existingProcedure);
                //remember to pass in false since this procedure is already ID mapped
                procedureBuilder.setDeletedAudit(activeCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
            }

            return;
        }

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterId();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);

        if (personId == null) {
            //TransformWarnings.log(LOG, parser, "Skipping Procedure {} due to missing encounter", procedureIdCell.getString());
            return;
        }

        // create the FHIR Procedure
        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);
        String conceptCode ;


        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        procedureBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);
//TODO should we use procedure data time? Mehbs said may be bad.
        CsvCell personIdCell = CsvCell.factoryDummyWrapper(personId);
        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);

        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)) {
            Date d = BartsCsvHelper.parseDate(procedureDateTimeCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);

        } else {
            //if there's no datetime, we've been told to ignore these records
            return;
        }

        Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!BartsCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        CsvCell personnelIdCell = parser.getPersonnelId();
//        //should I use the practitioner reference at all?  Never populated for Barts.
//        if (!BartsCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
//            Reference practitionerReference = csvHelper.createPractitionerReference(personnelIdCell);
//            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
//        }

        // Procedure is coded either Snomed or OPCS4
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
            String conceptCodeType = csvHelper.getProcedureOrDiagnosisConceptCodeType(conceptIdentifierCell);

            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

            if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_SNOMED)) {
                //NOTE: this code IS a SNOMED concept ID, unlike the Problem file which has a description ID
                String term = TerminologyService.lookupSnomedTerm(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find Snomed term for {}", conceptIdentifierCell);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_ICD_10)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find ICD-10 term for {}", conceptIdentifierCell);
                }

                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in a cell as this was derived
                codeableConceptBuilder.setText(term); //don't pass in a cell as this was derived

            } else if (conceptCodeType.equalsIgnoreCase(BartsCsvHelper.CODE_TYPE_OPCS_4)) {
                String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
                if (Strings.isNullOrEmpty(term)) {
                    TransformWarnings.log(LOG, csvHelper, "Failed to find OPCS-4 term for {}", conceptIdentifierCell);
                }
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptIdentifierCell);
                codeableConceptBuilder.setCodingCode(conceptCode, conceptIdentifierCell);
                codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                codeableConceptBuilder.setText(term); //don't pass in the cell as this is derived
            } else {
                throw new TransformException("Unknown PROCE code type [" + conceptCodeType + "]");
            }

        } else {
            //if there's no code, there's nothing to save
            return;
        }

        //TODO - match to fixed-width Procedure file, using Person ID (or similar), Code (need to break down the ConceptCodeIdentifier into scheme and code) and Date
        //populate comments, performed date, consultant etc. from that file if possible
        CsvCell sequenceNumberCell = parser.getCDSSequence();
        if (!BartsCsvHelper.isEmptyOrIsZero(sequenceNumberCell)) {
            procedureBuilder.setIsPrimary(true, sequenceNumberCell);
        }

        if (parser.getEncounterId() != null && parser.getEncounterId().getString() != null && parser.getProcedureTypeCode() != null) {
            String compatibleEncId = parser.getEncounterId().getString() + TWO_DECIMAL_PLACES; //Procedure has encounter ids suffixed with .00.
            ProcedurePojo pojo = csvHelper.getProcedureCache().getProcedurePojoByMultipleFields(compatibleEncId, personId, conceptIdentifierCell.getString(),
                    procedureDateTimeCell.getDate());
            if (pojo != null) {
                if (pojo.getProcedureCodeValueText().equals(parser.getProcedureTypeCode().getString())) {
                    if (pojo.getConsultant() != null) {
                        CsvCell consultantCell = pojo.getConsultant();
                        if (!consultantCell.isEmpty()) {
                            String consultantStr = consultantCell.getString();
                            String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, consultantStr);
                            if (Strings.isNullOrEmpty(personnelIdStr)) {
                                TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", consultantStr);
                            } else {
                                Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
                                procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
                            }
                        }
                    }
                    if (pojo.getNotes() != null && !pojo.getNotes().isEmpty()) {
                        procedureBuilder.addNotes(pojo.getNotes().getString());
                    }
                    if (pojo.getCreate_dt_tm() != null && pojo.getCreate_dt_tm().getDate() != null) {
                        procedureBuilder.setRecordedDate(pojo.getCreate_dt_tm().getDate());
                    }
                    if (pojo.getUpdatedBy() != null && pojo.getCreate_dt_tm().getDate() != null) {
                        CsvCell updateByCell = pojo.getUpdatedBy();
                        if (!updateByCell.isEmpty()) {
                            String updatedByStr = updateByCell.getString();
                            String personnelIdStr = csvHelper.getInternalId(PRSNLREFTransformer.MAPPING_ID_PERSONNEL_NAME_TO_ID, updatedByStr);
                            if (Strings.isNullOrEmpty(personnelIdStr)) {
                                TransformWarnings.log(LOG, csvHelper, "Failed to find PRSNL ID for {}", updatedByStr);
                            } else {
                                Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, personnelIdStr);
                                procedureBuilder.setRecordedBy(practitionerReference, personnelIdCell);
                            }
                        }
                    }
                }
            }
        }

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }

}
