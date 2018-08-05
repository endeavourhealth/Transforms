package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


public class SRStaffMemberProfileTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        List<InternalIdMap> mappingsToSave = new ArrayList<>();

        try {
            AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        createResource((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper, threadPool, mappingsToSave);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }

            if (!mappingsToSave.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(new Task(mappingsToSave, csvHelper));
                handleErrors(errors);
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        throw new TransformException("", exception);
    }

    private static void createResource(SRStaffMemberProfile parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper,
                                       ThreadPool threadPool,
                                       List<InternalIdMap> mappingsToSave) throws Exception {

        CsvCell staffProfileIdCell = parser.getRowIdentifier();

        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        StaffMemberProfilePojo staffPojo = new StaffMemberProfilePojo();

        staffPojo.setStaffMemberProfileIdCell(staffProfileIdCell);

        CsvCell orgId = parser.getIDOrganisation();
        if (!orgId.isEmpty()) { //shouldn't really happen, but there are a small number, so leave them without an org reference
            staffPojo.setIdOrganisation(orgId.getString());
        }

        CsvCell roleStart = parser.getDateEmploymentStart();
        if (!roleStart.isEmpty()) {
            staffPojo.setDateEmploymentStart(roleStart.getDate());
        }

        CsvCell roleEnd = parser.getDateEmploymentEnd();
        if (!roleEnd.isEmpty()) {
            staffPojo.setDateEmploymentEnd(roleEnd.getDate());
        }

        CsvCell roleName = parser.getStaffRole();
        if (!roleName.isEmpty()) {
            staffPojo.setStaffRole(roleName.getString());
        }

        CsvCell ppaId = parser.getPPAID();
        if (!ppaId.isEmpty()) {
            staffPojo.setPpaid(ppaId.getString());
        }

        CsvCell gpLocalCode = parser.getGPLocalCode();
        if (!gpLocalCode.isEmpty()) {
            staffPojo.setGpLocalCode(gpLocalCode.getString());
        }

        CsvCell gmpCode = parser.getGmpID();
        if (!gmpCode.isEmpty()) {
            staffPojo.setGmpId(gmpCode.getString());
        }

        CsvCell removedDataCell = parser.getRemovedData();
        //note this column isn't present on all versions, so we need to handle the cell being null
        if (removedDataCell != null && removedDataCell.getIntAsBoolean()) {
            staffPojo.setDeleted(true);
        }

        //We have the pojo so write it out
        csvHelper.getStaffMemberProfileCache().addStaffPojo(staffId, staffPojo);

        InternalIdMap mapping = new InternalIdMap();
        mapping.setServiceId(csvHelper.getServiceId());
        mapping.setIdType(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID);
        mapping.setSourceId(staffProfileIdCell.getString());
        mapping.setDestinationId(staffId.getString());

        mappingsToSave.add(mapping);

        if (mappingsToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<InternalIdMap> copy = new ArrayList<>(mappingsToSave);
            mappingsToSave.clear();

            List<ThreadPoolError> errors = threadPool.submit(new Task(copy, csvHelper));
            handleErrors(errors);
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
