package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ProcedureRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRecall;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ProcedureRequest;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Map;

public class SRRecallTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRRecallTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRecall.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRRecall)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRRecall parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell recallId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        ProcedureRequestBuilder procedureRequestBuilder = new ProcedureRequestBuilder();
        TppCsvHelper.setUniqueId(procedureRequestBuilder, patientId, recallId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        procedureRequestBuilder.setPatient(patientReference, patientId);

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), procedureRequestBuilder);
            return;
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            procedureRequestBuilder.setRecordedDateTime(dateRecored.getDate(), dateRecored);
        }

        CsvCell recallDate = parser.getDateRecall();
        if (!recallDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(recallDate.getDate());
            procedureRequestBuilder.setScheduledDate(dateTimeType, recallDate);
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {
            String staffMemberId = csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    recordedBy.getString());

            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            procedureRequestBuilder.setRecordedBy(staffReference, recordedBy);
        }

        CsvCell clinicianDoneBy = parser.getIDDoneBy();
        if (!clinicianDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(clinicianDoneBy);
            procedureRequestBuilder.setPerformer(staffReference, clinicianDoneBy);
        }

        CsvCell recallType = parser.getRecallType();
        // this is free text only with no code
        if (!recallType.isEmpty()) {
            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(procedureRequestBuilder,  null);
            codeableConceptBuilder.setText(recallType.getString());
        }

        CsvCell recallStatus = parser.getRecallStatus();
        // these are locally configured statues not mapped
        if (!recallStatus.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(recallStatus.getLong());
            String mappedTerm = tppMappingRef.getMappedTerm();
            if (!mappedTerm.isEmpty()) {

                // use the term to derive the resource status
                procedureRequestBuilder.setStatus(convertRecallStatus(mappedTerm));

                // add the status date and details to the notes
                CsvCell statusDate = parser.getRecallStatusDate();
                DateTimeType dateTimeType = new DateTimeType(statusDate.getDate());
                if (dateTimeType != null) {
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
                    String displayDate = sdf.format(dateTimeType);
                    String statusNote = "Status: " + displayDate + " - " + mappedTerm;
                    procedureRequestBuilder.addNotes(statusNote);
                }
            }
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            procedureRequestBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), procedureRequestBuilder);
    }

    private static ProcedureRequest.ProcedureRequestStatus convertRecallStatus(String recallStatusDisplay) {

        if (recallStatusDisplay.toLowerCase().contains("seen")) {
            return ProcedureRequest.ProcedureRequestStatus.COMPLETED;
        } else if (recallStatusDisplay.toLowerCase().contains("waiting")
                || recallStatusDisplay.toLowerCase().contains("recall")) {
            return ProcedureRequest.ProcedureRequestStatus.REQUESTED;
        } else if (recallStatusDisplay.toLowerCase().contains("cancelled")) {
            return ProcedureRequest.ProcedureRequestStatus.ABORTED;
        } else {
            return ProcedureRequest.ProcedureRequestStatus.NULL;
        }
    }

}
