package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Procedure;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedureTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

           for (ParserI parser: parsers) {
                try {

                    while (parser.nextRecord()) {
                        //no try/catch here, since any failure here means we don't want to continue
                        processRecord((Procedure) parser, fhirResourceFiler, csvHelper);
                    }
                } catch (Exception ex) {

                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
    }

    public static void processRecord(Procedure parser, FhirResourceFiler fhirResourceFiler, HomertonHiCsvHelper csvHelper) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        CsvCell procedureIdCell = parser.getProcedureId();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        Reference patientReference
                = ReferenceHelper.createReference(ResourceType.Patient, personEmpiIdCell.getString());
        procedureBuilder.setPatient(patientReference, personEmpiIdCell);

        //NOTE:deletions are checked by comparing the deletion hash values set up in the deletion pre-transform
        CsvCell hashValueCell = parser.getHashValue();
        boolean deleted = false;  //TODO: requires pre-transform per file to establish deletions
        if (deleted) {
            procedureBuilder.setDeletedAudit(hashValueCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureBuilder);
            return;
        }

        DateTimeType procedureDateTime = new DateTimeType(parser.getProcedureStartDate().getDateTime());
        procedureBuilder.setPerformed(procedureDateTime);

        CsvCell procedureEndDateCell = parser.getProcedureEndDate();
        if (!procedureEndDateCell.isEmpty()) {
            DateTimeType dt = new DateTimeType(procedureEndDateCell.getDateTime());
            procedureBuilder.setEnded(dt);
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        if (!encounterIdCell.isEmpty()) {

            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            procedureBuilder.setEncounter(encounterReference);
        }

        // coded concept
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(procedureBuilder, CodeableConceptBuilder.Tag.Procedure_Main_Code);

        // can be either of these types
        CsvCell procedureCodeSystemCell = parser.getProcedureCodingSystem();
        CsvCell procedureCodeCell = parser.getProcedureRawCode();
        if (procedureCodeSystemCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_SNOMED_URN)) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, procedureCodeSystemCell);
            codeableConceptBuilder.setCodingCode(procedureCodeCell.getString(), procedureCodeCell);

            CsvCell procedureDisplayTermCell = parser.getProcedureDisplayTerm();
            codeableConceptBuilder.setCodingDisplay(procedureDisplayTermCell.getString(), procedureDisplayTermCell);

        } else if (procedureCodeSystemCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_FREETEXT)) {

            //nothing to do here as text is set further down
        } else {

            throw new TransformException("Unknown Procedure code system [" + procedureCodeSystemCell.getString() + "]");
        }
        CsvCell procedureDescriptionCell = parser.getProcedureDescription();
        if (!procedureDescriptionCell.isEmpty()) {

            codeableConceptBuilder.setText(procedureDescriptionCell.getString());
        }

        //get any procedure comments text set during the pre-transform
        CsvCell procedureCommentsCell = csvHelper.findProcedureCommentText(procedureIdCell);
        if (!procedureCommentsCell.isEmpty()) {

            procedureBuilder.addNotes(procedureCommentsCell.getString(), procedureCommentsCell);
        }

        //TODO:  evaluate live PLACE_OF_SERVICE details to potentially map to referenced pre-transformed location

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }
}