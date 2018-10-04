package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRPrimaryCareMedication;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPrimaryCareMedicationPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPrimaryCareMedicationPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRPrimaryCareMedication.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRPrimaryCareMedication) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRPrimaryCareMedication parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) {
        //if this record is deleted, skip it
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {
            return;
        }

        CsvCell id = parser.getRowIdentifier();

        CsvCell eventIdCell = parser.getIDEvent();
        if (!eventIdCell.isEmpty()) {

            //depending on certain fields, we'll create MedicationOrders, MedicationStatements or both
            CsvCell isRepeatMedication = parser.getIsRepeatMedication();
            CsvCell isOtherMedication = parser.getIsOtherMedication();
            CsvCell isHospitalMedication = parser.getIsHospitalMedication();
            CsvCell isDentalMedication = parser.getIsDentalMedication();

            //create a medication statement for anything but repeats
            if (!isRepeatMedication.getBoolean()) {
                csvHelper.cacheNewConsultationChildRelationship(eventIdCell, id.getString(), ResourceType.MedicationStatement);
            }

            //create medication order for acutes and repeats (i.e. not these types)
            if (!isDentalMedication.getBoolean()
                    && !isHospitalMedication.getBoolean()
                    && !isOtherMedication.getBoolean()) {

                csvHelper.cacheNewConsultationChildRelationship(eventIdCell, id.getString(), ResourceType.MedicationOrder);
            }
        }


    }
}

