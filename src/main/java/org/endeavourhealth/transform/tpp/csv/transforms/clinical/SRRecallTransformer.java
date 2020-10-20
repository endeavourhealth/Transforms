package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRecall;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRRecallTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRRecallTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRecall.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRRecall) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRRecall parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell recallId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) { //null check required as the column didn't always exist

            // get previously filed resource for deletion
            ProcedureRequest procedureRequest = (ProcedureRequest)csvHelper.retrieveResource(recallId.getString(), ResourceType.ProcedureRequest);

            if (procedureRequest != null) {
                ProcedureRequestBuilder procedureRequestBuilder = new ProcedureRequestBuilder(procedureRequest);
                procedureRequestBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, procedureRequestBuilder);
            }
            return;
        }

        ProcedureRequestBuilder procedureRequestBuilder = new ProcedureRequestBuilder();
        procedureRequestBuilder.setId(recallId.getString(), recallId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        procedureRequestBuilder.setPatient(patientReference, patientId);

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            procedureRequestBuilder.setRecordedDateTime(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell recallDate = parser.getDateRecall();
        if (!recallDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(recallDate.getDate());
            procedureRequestBuilder.setScheduledDate(dateTimeType, recallDate);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            procedureRequestBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        Reference doneByReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgDoneAtCell);
        if (doneByReference != null) {
            procedureRequestBuilder.setPerformer(doneByReference, staffMemberIdDoneByCell, orgDoneAtCell);
        }

        CsvCell recallType = parser.getRecallType();
        // this is free text only with no code
        if (!recallType.isEmpty()) {
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(procedureRequestBuilder, CodeableConceptBuilder.Tag.Procedure_Request_Main_Code);
            codeableConceptBuilder.setText(recallType.getString());
        }

        CsvCell recallStatusCell = parser.getRecallStatus();
        // these are locally configured statues not mapped
        if (!recallStatusCell.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recallStatusCell);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (!mappedTerm.isEmpty()) {

                    // use the term to derive the resource status
                    ProcedureRequest.ProcedureRequestStatus fhirStatus = convertRecallStatus(mappedTerm);
                    procedureRequestBuilder.setStatus(fhirStatus, recallStatusCell);

                    // add the status date and details to the notes
                    CsvCell statusDate = parser.getRecallStatusDate();
                    DateType dateType = new DateType(statusDate.getDate());
                    if (dateType != null) {
                        String displayDate = dateType.toHumanDisplay();
                        String statusNote = "Status: " + displayDate + " - " + mappedTerm;
                        procedureRequestBuilder.addNotes(statusNote);
                    }
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId);
            procedureRequestBuilder.setEncounter(eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureRequestBuilder);
    }

    /**
     * to get a full list of TPP options in this value set, run this on publisher_common:
             select *
             from tpp_mapping_ref_2
             where group_id = 15777;
     * This list hasn't changed in years, so the mapping is just a hard-coded lookup
     */
    private static ProcedureRequest.ProcedureRequestStatus convertRecallStatus(String str) throws Exception {

        if (str.equalsIgnoreCase("Pending")) {
            return ProcedureRequest.ProcedureRequestStatus.PROPOSED;
        } else if (str.equalsIgnoreCase("Seen")
                || str.equalsIgnoreCase("Awaiting Result")) {
            return ProcedureRequest.ProcedureRequestStatus.COMPLETED;
        } else if (str.equalsIgnoreCase("Cancelled by clinician")
                || str.equalsIgnoreCase("Cancelled by patient")
                || str.equalsIgnoreCase("Cancelled during import")
                || str.equalsIgnoreCase("Superseded")
                || str.equalsIgnoreCase("Suspended")) {
            return ProcedureRequest.ProcedureRequestStatus.REJECTED;
        } else if (str.equalsIgnoreCase("1st Recall")
                || str.equalsIgnoreCase("2nd Recall")
                || str.equalsIgnoreCase("3rd Recall")) {
            return ProcedureRequest.ProcedureRequestStatus.REQUESTED;
        } else {
            throw new Exception("Unmapped TPP SRRecall status [" + str + "]");
        }
    }

}
