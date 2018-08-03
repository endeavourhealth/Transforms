package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class SRStaffMemberPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        try {
            AbstractCsvParser parser = parsers.get(SRStaffMember.class);

            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRStaffMember) parser, fhirResourceFiler, csvHelper, threadPool);
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

    private static void processRecord(SRStaffMember parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper,
                                       ThreadPool threadPool) throws Exception {

        //we need to pre-cache the role details of all practitioners that we're going to update
        //since it's possible to receive an update to SRStaffMember without receiving any of the SRStaffMemberProfile records
        //and we don't want to lose the roles
        CsvCell staffMemberIdCell = parser.getRowIdentifier();



//TODO - retrieve practitioner from DB because we don't want to lose any roles for each of them (do this in pre-transformer???)
//TODO - make sure it's trhead safe!!!
//TODO - make sure to handle duplicates between this class and the profile transformer


        Task task = new Task(csvHelper, staffMemberIdCell);
        List<ThreadPoolError> errors = threadPool.submit(task);
        handleErrors(errors);
    }


    static class Task implements Callable {

        private TppCsvHelper csvHelper;
        private CsvCell staffMemberIdCell;

        public Task(TppCsvHelper csvHelper, CsvCell staffMemberIdCell) {
            this.csvHelper = csvHelper;
            this.staffMemberIdCell = staffMemberIdCell;
        }

        @Override
        public Object call() throws Exception {

            try {
                Practitioner practitioner = (Practitioner)csvHelper.retrieveResource(staffMemberIdCell.getString(), ResourceType.Practitioner);
                if (practitioner != null
                        && practitioner.hasPractitionerRole()) {

                    for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {

                        StaffMemberProfilePojo cache = new StaffMemberProfilePojo();

                        CsvCell profileIdCell = CsvCell.factoryDummyWrapper(role.getId());
                        cache.setStaffMemberProfileIdCell(profileIdCell);

                        if (role.hasRole()) {
                            CodeableConcept cc = role.getRole();
                            Coding coding = CodeableConceptHelper.findCoding(cc, FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
                            if (coding != null) {
                                cache.setStaffRole(coding.getDisplay());
                            }
                        }

                        if (role.hasPeriod()) {
                            Period period = role.getPeriod();
                            if (period.hasStart()) {
                                Date d = period.getStart();
                                cache.setDateEmploymentStart(d);
                            }

                            if (period.hasEnd()) {
                                Date d = period.getEnd();
                                cache.setDateEmploymentEnd(d);
                            }
                        }

                        if (role.hasManagingOrganization()) {
                            Reference orgReference = role.getManagingOrganization();
                            orgReference = IdHelper.convertEdsReferenceToLocallyUniqueReference(csvHelper, orgReference);
                            String orgId = ReferenceHelper.getReferenceId(orgReference);
                            cache.setIdOrganisation(orgId);
                        }

                        //TODO - carry over these???
                        /**
                         private String ppaid;
                         private String gpLocalCode;
                         private String gmpId;
                         */

                        csvHelper.getStaffMemberProfileCache().addStaffPojo(staffMemberIdCell, cache);
                    }
                }

            } catch (Throwable t) {
                LOG.error("Error caching existing roles for staff member " + staffMemberIdCell, t);
                throw t;
            }

            return null;
        }
    }

}
