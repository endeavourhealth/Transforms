package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SREvent;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SREventPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SREventPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(SREvent.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        transform((SREvent) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        throw new TransformException(parser.getCurrentState().toString(), ex);
                    }
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(SREvent parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        CsvCell removedData = parser.getRemovedData();
        if (removedData != null && removedData.getIntAsBoolean()) {
            return;
        }

        CsvCell consultationGuid = parser.getRowIdentifier();
        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell organisationDoneAtCell = parser.getIDOrganisationDoneAt();

        CsvCurrentState parserState = parser.getCurrentState();

        LookupTask task = new LookupTask(parserState, consultationGuid, profileIdRecordedByCell, staffMemberIdDoneByCell, organisationDoneAtCell, csvHelper);
        csvHelper.submitToThreadPool(task);
    }

    static class LookupTask extends AbstractCsvCallable {

        private CsvCell encounterIdCell;
        private CsvCell profileIdRecordedByCell;
        private CsvCell staffMemberIdDoneByCell;
        private CsvCell organisationDoneAtCell;
        private TppCsvHelper csvHelper;

        public LookupTask(CsvCurrentState parserState,
                          CsvCell encounterIdCell,
                          CsvCell profileIdRecordedByCell,
                          CsvCell staffMemberIdDoneByCell,
                          CsvCell organisationDoneAtCell,
                          TppCsvHelper csvHelper) {

            super(parserState);
            this.encounterIdCell = encounterIdCell;
            this.profileIdRecordedByCell = profileIdRecordedByCell;
            this.staffMemberIdDoneByCell = staffMemberIdDoneByCell;
            this.organisationDoneAtCell = organisationDoneAtCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                //we don't transform Practitioners until we need them, and these ensure it happens
                if (!profileIdRecordedByCell.isEmpty()
                        && profileIdRecordedByCell.getLong().longValue() > 0) {
                    csvHelper.getStaffMemberCache().ensurePractitionerIsTransformedForStaffProfileId(profileIdRecordedByCell);
                }

                if (!staffMemberIdDoneByCell.isEmpty()
                        && staffMemberIdDoneByCell.getLong().longValue() > 0) {
                    csvHelper.getStaffMemberCache().ensurePractitionerIsTransformedForStaffMemberId(staffMemberIdDoneByCell, csvHelper);
                }

                //carry over linked items from any previous instance of this Consultation
                Encounter previousVersion = (Encounter)csvHelper.retrieveResource(encounterIdCell.getString(), ResourceType.Encounter);
                if (previousVersion != null) { //if this is the first time, then we'll have a null resource

                    EncounterBuilder encounterBuilder = new EncounterBuilder(previousVersion);
                    ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

                    List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getReferences();

                    //the references will be mapped to Discovery UUIDs, so we need to convert them back to local IDs
                    List<Reference> previousReferencesLocalIds = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, previousReferencesDiscoveryIds);

                    csvHelper.cacheConsultationPreviousLinkedResources(encounterIdCell, previousReferencesLocalIds);
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}