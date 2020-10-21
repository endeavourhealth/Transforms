package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Procedure;
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

        //TODO - the rest!



        //get any procedure comments text set during the pre-transform
        CsvCell procedureCommentsCell = csvHelper.findProcedureCommentText(procedureIdCell);
        if (!procedureCommentsCell.isEmpty()) {

            procedureBuilder.addNotes(procedureCommentsCell.getString());
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureBuilder);
    }
}