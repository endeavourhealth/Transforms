package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRSpecialNotes;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRSpecialNotesTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRSpecialNotesTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRSpecialNotes.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRSpecialNotes) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRSpecialNotes parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            Flag flag = (Flag)csvHelper.retrieveResource(rowId.getString(), ResourceType.Flag);
            if (flag != null) {
                FlagBuilder flagBuilder = new FlagBuilder(flag);
                flagBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, flagBuilder);
            }
            return;
        }

        FlagBuilder flagBuilder = new FlagBuilder();
        flagBuilder.setId(rowId.getString(), rowId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        flagBuilder.setSubject(patientReference, patientId);

        CsvCell startDate = parser.getDateStart();
        if (!startDate.isEmpty()) {

            flagBuilder.setStartDate(startDate.getDate(), startDate);
        }

        CsvCell noteType = parser.getType();
        if (!noteType.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(noteType);
            if (tppMappingRef != null) {
                String mappedTerm = tppMappingRef.getMappedTerm();
                if (!mappedTerm.isEmpty()) {
                    flagBuilder.setCategory(mappedTerm, noteType);
                }
            }
        }

        CsvCell noteText = parser.getNote();
        if (!noteText.isEmpty()) {

            flagBuilder.setCode(noteText.getString(), noteText);
        }

        CsvCell expiredDate = parser.getDateExpired();
        if (!expiredDate.isEmpty()) {
            flagBuilder.setEndDate(expiredDate.getDate(), expiredDate);
            flagBuilder.setStatus(Flag.FlagStatus.INACTIVE);

        } else {
            flagBuilder.setStatus(Flag.FlagStatus.ACTIVE);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            flagBuilder.setAuthor(recordedByReference, profileIdRecordedByCell);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}