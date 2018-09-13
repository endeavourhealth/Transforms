package org.endeavourhealth.transform.homerton.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.ProcedureTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ProcedureTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                        createProcedure((ProcedureTable) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createProcedure(ProcedureTable parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonCsvHelper csvHelper) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureID();

        //if the record is non-active (i.e. deleted) we ONLY get the ID, date and active indicator, NOT the person ID
        //so we need to re-retrieve the previous instance of the resource to find the patient Reference which we need to delete
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            Procedure existingProcedure = (Procedure)csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureIdCell.getString());
            if (existingProcedure != null) {
                ProcedureBuilder procedureBuilder = new ProcedureBuilder(existingProcedure);
                //remember to pass in false since this procedure is already ID mapped
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
            }

            return;
        }

        // get encounter details (should already have been created previously)
        CsvCell encounterIdCell = parser.getEncounterID();
        String personId = csvHelper.findPersonIdFromEncounterId(encounterIdCell);

        if (personId == null) {
            //TransformWarnings.log(LOG, parser, "Skipping Procedure {} due to missing encounter", procedureIdCell.getString());
            return;
        }

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);

        Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personId);
        procedureBuilder.setPatient(patientReference); //we don't have a source cell to audit with, since this came from the Encounter

        CsvCell procedureDateTimeCell = parser.getProcedureDateTime();
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(procedureDateTimeCell)) {
            Date d = BartsCsvHelper.parseDate(procedureDateTimeCell);
            DateTimeType dateTimeType = new DateTimeType(d);
            procedureBuilder.setPerformed(dateTimeType, procedureDateTimeCell);
        }

        procedureBuilder.setStatus(Procedure.ProcedureStatus.COMPLETED);

        Reference encounterReference
                = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
        procedureBuilder.setEncounter(encounterReference, encounterIdCell);

        CsvCell encounterSliceIdCell = parser.getEncounterSliceID();
        if (!HomertonCsvHelper.isEmptyOrIsZero(encounterSliceIdCell)) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(procedureBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_ENCOUNTER_SLICE_ID);
            identifierBuilder.setValue(encounterSliceIdCell.getString(), encounterSliceIdCell);
        }

        //TODO - need personnel data
//        CsvCell personnelIdCell = parser.getPersonnelId();
//        if (!HomertonCsvHelper.isEmptyOrIsZero(personnelIdCell)) {
//            Reference practitionerReference = csvHelper.createPractitionerReference(personnelIdCell);
//            procedureBuilder.addPerformer(practitionerReference, personnelIdCell);
//        }

        // Procedure is coded either as Snomed or OPCS4
        CsvCell conceptCodeCell = parser.getConceptCode();
        if (!conceptCodeCell.isEmpty()) {

            String conceptCode = conceptCodeCell.getString();
            CsvCell conceptCodeTypeCell = parser.getConceptCodeType();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

            if (!conceptCodeTypeCell.isEmpty()) {

                String conceptCodeType = conceptCodeTypeCell.getString();
                if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_SNOMED)) {

                    // Homerton use Snomed descriptionId instead of conceptId
                    SnomedCode snomedCode = TerminologyService.lookupSnomedConceptForDescriptionId(conceptCode);
                    if (snomedCode == null) {
                        TransformWarnings.log(LOG, parser, "Failed to find Snomed term for {}", conceptCodeCell);

                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_DESCRIPTION_ID, conceptCodeTypeCell);
                        codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);

                    } else {
                        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conceptCodeTypeCell);
                        codeableConceptBuilder.setCodingCode(snomedCode.getConceptCode(), conceptCodeCell);
                        codeableConceptBuilder.setCodingDisplay(snomedCode.getTerm()); //don't pass in the cell as this is derived
                        CsvCell term = parser.getProcedureDesc();
                        if (!term.isEmpty()) {
                            codeableConceptBuilder.setText(term.getString(), term);
                        }
                    }

                } else if (conceptCodeType.equalsIgnoreCase(HomertonCsvHelper.CODE_TYPE_OPCS_4)) {
                    String term = TerminologyService.lookupOpcs4ProcedureName(conceptCode);
                    if (Strings.isNullOrEmpty(term)) {
                        TransformWarnings.log(LOG, parser, "Failed to find OPCS4 term for {}", conceptCodeCell);
                    }

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, conceptCodeTypeCell);
                    codeableConceptBuilder.setCodingCode(conceptCode, conceptCodeCell);
                    codeableConceptBuilder.setCodingDisplay(term); //don't pass in the cell as this is derived
                    CsvCell origTerm = parser.getProcedureDesc();
                    if (!origTerm.isEmpty()) {
                        codeableConceptBuilder.setText(origTerm.getString(), origTerm);
                    }

                } else {
                    throw new TransformException("Unknown Procedure code type [" + conceptCodeType + "]");
                }
            }
        } else {
            //if there's no code, create a non coded code so we retain the text
            CsvCell term = parser.getProcedureDesc();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);
            codeableConceptBuilder.setText(term.getString());
        }

        //TODO - check that this is right. In the Barts transform the procedure type was transformed like
        //this but it was useless information as the procedure type was always "procedure". If this is the
        //case then don't bother carrying this over, as we already know it's a procedure because of the resource type
        CsvCell procedureType = parser.getProcedureType();
        if (!procedureType.isEmpty()) {

            procedureBuilder.setCategory(procedureType.getString(), procedureType);
        }

        // save resource
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }
}
