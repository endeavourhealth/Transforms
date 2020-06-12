package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRImmunisation;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRImmunisationPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRDrugSensitivityPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRImmunisation.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRImmunisation) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRImmunisation parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) {
        //if this record is deleted, skip it
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {
            return;
        }


        //we don't transform Practitioners until we need them, and these ensure it happens
        CsvCell profileIdEnteredByCell = parser.getIDProfileEnteredBy();
        csvHelper.getStaffMemberCache().addRequiredProfileId(profileIdEnteredByCell);

        CsvCell staffIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        csvHelper.getStaffMemberCache().addRequiredStaffId(staffIdDoneByCell, orgDoneAtCell);


        CsvCell id = parser.getRowIdentifier();

        CsvCell eventIdCell = parser.getIDEvent();
        if (!eventIdCell.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(eventIdCell, id.getString(), ResourceType.Immunization);
        }
    }
}

