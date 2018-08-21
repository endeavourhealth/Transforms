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

        CsvCell rowIdCell = parser.getRowIdentifier();
        if ((rowIdCell.isEmpty()) || (!StringUtils.isNumeric(rowIdCell.getString()))) {
            TransformWarnings.log(LOG, parser, "ERROR: invalid row Identifier: {} in file : {}", rowIdCell.getString(), parser.getFilePath());
            return;
        }

        CsvCell removeDataCell = parser.getRemovedData();
        if (removeDataCell != null && removeDataCell.getIntAsBoolean()) {
            return;
        }

        CsvCell dateEvent = parser.getDateEvent();
        CsvCell medicalRecordStatusCell = parser.getMedicalRecordStatus();
        CsvCell patientId = parser.getIDPatient();

        if (!medicalRecordStatusCell.isEmpty() && !dateEvent.isEmpty()) {
            csvHelper.cacheMedicalRecordStatus(patientId, dateEvent, medicalRecordStatusCell);
        }
    }
}