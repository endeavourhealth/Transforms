package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.StaffMemberProfileCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        try {
            AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        createResource((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper, threadPool);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
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
                                       ThreadPool threadPool) throws Exception {

        //

        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        StaffMemberProfilePojo staffPojo = new StaffMemberProfilePojo();
        staffPojo.setIDStaffMember(staffId.getLong()); // Save id as Long for use as key.
        //staffPojo.setIDStaffMemberCell(staffId);   // and as CsvCell for builders.

        CsvCell staffMemberProfileId = parser.getRowIdentifier();
        staffPojo.setRowIdentifier(staffMemberProfileId);

        CsvCell orgId = parser.getIDOrganisation();
        if (!orgId.isEmpty()) { //shouldn't really happen, but there are a small number, so leave them without an org reference
            staffPojo.setIDOrganisation(orgId.getString());
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
            staffPojo.setPPAID(ppaId.getString());
        }

        CsvCell gpLocalCode = parser.getGPLocalCode();
        if (!gpLocalCode.isEmpty()) {
            staffPojo.setGPLocalCode(gpLocalCode.getString());
        }

        CsvCell gmpCode = parser.getGmpID();
        if (!gmpCode.isEmpty()) {
            staffPojo.setGmpID(gmpCode.getString());
        }

        CsvCell idProfileCreatedBy = parser.getIdProfileCreatedBy();
        if (!idProfileCreatedBy.isEmpty() && idProfileCreatedBy.getLong() > 0) {
            staffPojo.setIdProfileCreatedBy(idProfileCreatedBy.getString());
        }

        CsvCell dateProfileCreated = parser.getDateProfileCreated();
        if (!dateProfileCreated.isEmpty()) {
            staffPojo.setDateProfileCreated(dateProfileCreated.getDate());
        }

        CsvCell idStaffMemberProfileRole = parser.getIDStaffMemberProfileRole();
        if (!idStaffMemberProfileRole.isEmpty()) {
            staffPojo.setIDStaffMemberProfileRole(idStaffMemberProfileRole.getString());
        }

        staffPojo.setParserState(parser.getCurrentState());

        //We have the pojo so write it out
        StaffMemberProfileCache.addStaffPojo(staffPojo);
        if ((StaffMemberProfileCache.size())%10000==0) { //Cache size every 10k records
            LOG.info("Staff member profile cache at " + StaffMemberProfileCache.size());
        }

        Task task = new Task(csvHelper, staffId, idStaffMemberProfileRole);
        List<ThreadPoolError> errors = threadPool.submit(task);
        handleErrors(errors);
    }

    static class Task implements Callable {

        private TppCsvHelper csvHelper;
        private CsvCell staffMemberIdCell;
        private CsvCell staffProfileIdCell;

        public Task(TppCsvHelper csvHelper, CsvCell staffMemberIdCell, CsvCell staffProfileIdCell) {
            this.csvHelper = csvHelper;
            this.staffMemberIdCell = staffMemberIdCell;
            this.staffProfileIdCell = staffProfileIdCell;
        }

        @Override
        public Object call() throws Exception {

            try {
                csvHelper.saveInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                                            staffProfileIdCell.getString(), staffMemberIdCell.getString());

            } catch (Throwable t) {
                LOG.error("Error saving internal ID from " + staffProfileIdCell + " to " + staffMemberIdCell , t);
                throw t;
            }

            return null;
        }
    }
}
