package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PROCETransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCETransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createProcedure((PROCE)parser, fhirResourceFiler, csvHelper);
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

            Procedure existingProcedure = (Procedure)csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureIdCell);
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

        // set the patient reference
        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        procedureBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

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
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
            Reference practitionerReference = csvHelper.createPractitionerReference(personnelIdCell);
            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
        }

        // Procedure is coded either Snomed or OPCS4
        CsvCell conceptIdentifierCell = parser.getConceptCodeIdentifier();
        if (!conceptIdentifierCell.isEmpty()) {
            String conceptCode = csvHelper.getProcedureOrDiagnosisConceptCode(conceptIdentifierCell);
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
        }

        //TODO - match to fixed-width Procedure file, using Person ID (or similar), Code (need to break down the ConceptCodeIdentifier into scheme and code) and Date
        //populate comments, performed date, consultant etc. from that file if possible

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }

}
