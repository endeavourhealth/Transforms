package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOut;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRReferralOutPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRDrugSensitivityPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralOut.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processRecord((SRReferralOut) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRReferralOut parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {
        //if this record is deleted, skip it
        CsvCell removedCell = parser.getRemovedData();
        if (removedCell != null && removedCell.getIntAsBoolean()) {
            return;
        }


        //we don't transform Practitioners until we need them, and these ensure it happens
        CsvCell profileIdEnteredByCell = parser.getIDProfileEnteredBy();
        csvHelper.getStaffMemberCache().addRequiredProfileId(profileIdEnteredByCell);

        //ReferralOut file is unique in that it has a second profile ID column
        CsvCell profileReferredByCell = parser.getIDProfileReferrer();
        csvHelper.getStaffMemberCache().addRequiredProfileId(profileReferredByCell);

        CsvCell staffIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        csvHelper.getStaffMemberCache().addRequiredStaffId(staffIdDoneByCell, orgDoneAtCell);


        CsvCell id = parser.getRowIdentifier();

        CsvCell eventLinkId = parser.getIDEvent();
        if (!eventLinkId.isEmpty()) {
            csvHelper.cacheNewConsultationChildRelationship(eventLinkId, id.getString(), ResourceType.ReferralRequest);
        }
    }
}

