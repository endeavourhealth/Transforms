package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.FlagBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRChildAtRisk;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRChildAtRiskTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRChildAtRiskTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRChildAtRisk.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRChildAtRisk) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRChildAtRisk parser,
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

        CsvCell profieIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profieIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profieIdRecordedBy);
            flagBuilder.setAuthor(staffReference, profieIdRecordedBy);
        }

        CsvCell onPlan = parser.getChildProtectionPlan();
        if (!onPlan.isEmpty()) {
            if (onPlan.getBoolean()) {
                flagBuilder.setStatus(Flag.FlagStatus.ACTIVE, onPlan);
            } else {
                flagBuilder.setStatus(Flag.FlagStatus.INACTIVE, onPlan);
            }
        }

        flagBuilder.setCode("On Child Protection Plan");

        flagBuilder.setCategory("Clinical");

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), flagBuilder);
    }
}