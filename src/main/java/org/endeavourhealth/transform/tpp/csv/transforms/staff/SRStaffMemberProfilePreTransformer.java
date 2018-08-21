package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRStaffMemberProfilePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        List<InternalIdMap> mappingsToSave = new ArrayList<>();

        try {
            AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper, mappingsToSave);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }

            if (!mappingsToSave.isEmpty()) {
                csvHelper.submitToThreadPool(new Task(mappingsToSave, csvHelper));
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processRecord(SRStaffMemberProfile parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper,
                                      List<InternalIdMap> mappingsToSave) throws Exception {

        CsvCell staffProfileIdCell = parser.getRowIdentifier();

        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        InternalIdMap mapping = new InternalIdMap();
        mapping.setServiceId(csvHelper.getServiceId());
        mapping.setIdType(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID);
        mapping.setSourceId(staffProfileIdCell.getString());
        mapping.setDestinationId(staffId.getString());

        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<InternalIdMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();

            csvHelper.submitToThreadPool(new Task(copy, csvHelper));
        }
    }

    static class Task implements Callable {

        private List<InternalIdMap> mappings;
        private TppCsvHelper csvHelper;

        public Task(List<InternalIdMap> mappings, TppCsvHelper csvHelper) {
            this.mappings = mappings;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                csvHelper.saveInternalIds(mappings);

            } catch (Throwable t) {
                String msg = "Error saving internal ID maps for staff member profile IDs ";
                for (InternalIdMap mapping: mappings) {
                    msg += mapping.getSourceId();
                    msg += ", ";
                }

                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }
}

