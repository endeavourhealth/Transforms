package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRRecordStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRRecordStatusTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRRecordStatusTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRRecordStatus.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRRecordStatus) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRRecordStatus parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            return;
        }

        //we want to ignore any record status records for other organisations. For other files, we want to include
        //data from elsewhere, but these records are specific to the patients registration at other organisations,
        //so are irrelevant and confusing
        if (!SRPatientRegistrationTransformer.shouldSaveEpisode(parser.getIDOrganisation(), parser.getIDOrganisationVisibleTo())) {
            return;
        }

        CsvCell dateEventCell = parser.getDateEvent();
        CsvCell medicalRecordStatusCell = parser.getMedicalRecordStatus();
        CsvCell patientIdCell = parser.getIDPatient();
        csvHelper.getRecordStatusHelper().cacheMedicalRecordStatus(patientIdCell, dateEventCell, medicalRecordStatusCell);
    }
}