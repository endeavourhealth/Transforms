package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
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
        while (parser.nextRecord()) {

            try {
                createResource((SRRecordStatus)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
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
        if (removeDataCell.getIntAsBoolean()) {
            return;
        }

        CsvCell dateEvent = parser.getDateEvent();
        CsvCell medicalRecordStatus = parser.getMedicalRecordStatus();
        CsvCell patientId = parser.getIDPatient();
        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        if (!medicalRecordStatus.isEmpty() && !dateEvent.isEmpty()) {
            //TODO - medicalRecordStatus CsvCell needs to be cached here too, so that it can be passed into the EpisodeOfCareBuilder when the cached value is used
            csvHelper.cacheMedicalRecordStatus(patientId, dateEvent.getDate(), convertMedicalRecordStatus(medicalRecordStatus.getInt()));
        }
    }

    private static String convertMedicalRecordStatus(Integer medicalRecordStatus) throws Exception {
        switch (medicalRecordStatus) {
            case 0:
                return "No medical records";
            case 1:
                return "Medical records are on the way";
            case 2:
                return "Medical records here";
            case 3:
                return "Medical records sent";
            case 4:
                return "Medical records need to be sent";
            default:
                throw new TransformException("Unmapped medical record status " + medicalRecordStatus);
        }
    }
}