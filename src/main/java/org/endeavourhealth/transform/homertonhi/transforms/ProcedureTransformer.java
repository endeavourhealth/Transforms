package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Procedure;
import org.endeavourhealth.transform.homertonhi.schema.ProcedureDelete;
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
            if (parser != null) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                        continue;
                    }
                    try {
                        transform((Procedure) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void delete(List<ParserI> parsers,
                              FhirResourceFiler fhirResourceFiler,
                              HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        ProcedureDelete procedureDeleteParser = (ProcedureDelete) parser;
                        CsvCell hashValueCell = procedureDeleteParser.getHashValue();

                        //lookup the localId value set when the Procedure was initially transformed
                        String procedureId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(procedureId)) {
                            //get the resource to perform the deletion on
                            org.hl7.fhir.instance.model.Procedure procedure
                                    = (org.hl7.fhir.instance.model.Procedure) csvHelper.retrieveResourceForLocalId(ResourceType.Procedure, procedureId);

                            if (procedure != null) {

                                ProcedureBuilder procedureBuilder = new ProcedureBuilder(procedure);
                                procedureBuilder.setDeletedAudit(hashValueCell);

                                //delete the patient resource. mapids is always false for deletions
                                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureBuilder);
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Delete failed. Unable to find Procedure HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
                                    hashValueCell.toString());
                        }
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(Procedure parser, FhirResourceFiler fhirResourceFiler, HomertonHiCsvHelper csvHelper) throws Exception {

        ProcedureBuilder procedureBuilder = new ProcedureBuilder();

        CsvCell procedureIdCell = parser.getProcedureId();
        procedureBuilder.setId(procedureIdCell.getString(), procedureIdCell);

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        Reference patientReference
                = ReferenceHelper.createReference(ResourceType.Patient, personEmpiIdCell.getString());
        procedureBuilder.setPatient(patientReference, personEmpiIdCell);

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. procedure_id
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, procedureIdCell);

        CsvCell procedureStartDateCell = parser.getProcedureStartDate();
        if (!procedureStartDateCell.isEmpty()) {

            DateTimeType procedureDateTime = new DateTimeType(procedureStartDateCell.getDateTime());
            procedureBuilder.setPerformed(procedureDateTime, procedureStartDateCell);
        }

        CsvCell procedureEndDateCell = parser.getProcedureEndDate();
        if (!procedureEndDateCell.isEmpty()) {

            DateTimeType dt = new DateTimeType(procedureEndDateCell.getDateTime());
            procedureBuilder.setEnded(dt, procedureEndDateCell);
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        if (!encounterIdCell.isEmpty()) {

            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            procedureBuilder.setEncounter(encounterReference, encounterIdCell);
        }

        CsvCell rankTypeCell = parser.getRankType();
        if (!rankTypeCell.isEmpty()) {
            procedureBuilder.setIsPrimary(rankTypeCell.getString().equalsIgnoreCase("PRIMARY"), rankTypeCell);
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

        } else if (procedureCodeSystemCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_OPCS4_URN)) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_OPCS4, procedureCodeSystemCell);
            codeableConceptBuilder.setCodingCode(procedureCodeCell.getString(), procedureCodeCell);

            CsvCell procedureDisplayTermCell = parser.getProcedureDisplayTerm();
            codeableConceptBuilder.setCodingDisplay(procedureDisplayTermCell.getString(), procedureDisplayTermCell);

        } else if (procedureCodeSystemCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_FREETEXT)) {

            //nothing to do here as procedure free text is set further down
        } else {

            throw new TransformException("Unknown Procedure code system [" + procedureCodeSystemCell.getString() + "]");
        }
        CsvCell procedureDescriptionCell = parser.getProcedureDescription();
        if (!procedureDescriptionCell.isEmpty()) {

            codeableConceptBuilder.setText(procedureDescriptionCell.getString(), procedureDescriptionCell);
        }

        //get any procedure comments text set during the pre-transform
        CsvCell procedureCommentsCell = csvHelper.findProcedureCommentText(procedureIdCell);
        if (!procedureCommentsCell.isEmpty()) {

            procedureBuilder.addNotes(procedureCommentsCell.getString(), procedureCommentsCell);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }
}