package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRPersonAtRisk;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPersonAtRiskTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRPersonAtRiskTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPersonAtRisk.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRPersonAtRisk) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRPersonAtRisk parser,
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

        CsvCell dateAdded = parser.getDateAdded();
        if (!dateAdded.isEmpty()) {
            flagBuilder.setStartDate(dateAdded.getDate(), dateAdded);
        }

        CsvCell dateRemoved = parser.getDateRemoved();
        if (!dateRemoved.isEmpty()) {
            flagBuilder.setEndDate(dateRemoved.getDate(), dateRemoved);
        }

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            flagBuilder.setAuthor(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell onPlan = parser.getProtectionPlan();
        if (!onPlan.isEmpty()) {
            if (onPlan.getBoolean()) {
                flagBuilder.setStatus(Flag.FlagStatus.ACTIVE, onPlan);
            } else {
                flagBuilder.setStatus(Flag.FlagStatus.INACTIVE, onPlan);
            }
        }

        //add the plan reason to the code text if present
        CsvCell reasonForPlan = parser.getReasonForPlan();
        if (!reasonForPlan.isEmpty()) {
            flagBuilder.setCode("On Protection Plan. " + reasonForPlan.getString(), reasonForPlan);
        } else {
            flagBuilder.setCode("On Protection Plan");
        }

        flagBuilder.setCategory("Clinical");

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}