package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppStaffDalI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class SRStaffMemberProfileTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        SRStaffMemberProfile parser = (SRStaffMemberProfile)parsers.get(SRStaffMemberProfile.class);

        if (parser != null) {

            //we need to go through the file records to make sure it's audited and to find the RowIds
            //of the records that have changed
            while (parser.nextRecord()) {

                CsvCell profileId = parser.getRowIdentifier();
                CsvCell staffId = parser.getIDStaffMember();
                CsvCell orgId = parser.getIDOrganisation();
                csvHelper.getStaffMemberCache().addChangedProfileId(profileId, staffId, orgId);
            }

            //bulk load the file into the DB
            String filePath = parser.getFilePath();
            int fileId = parser.getFileAuditId().intValue();
            Date dataDate = fhirResourceFiler.getDataDate();
            TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
            dal.updateStaffProfileLookupTable(filePath, dataDate, fileId);
        }
    }

    /*private static void processRecord(SRStaffMemberProfile parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler,
                                        List<Record> staffToSave) throws Exception {

        //seems to be some bad data in this file, where we have a record that doesn't link to a staff member record
        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        CsvCell profileIdCell = parser.getRowIdentifier();
        CsvCell orgId = parser.getIDOrganisation();
        CsvCell removedDataCell = parser.getRemovedData();
        CsvCell roleStart = parser.getDateEmploymentStart();
        CsvCell roleEnd = parser.getDateEmploymentEnd();
        CsvCell roleName = parser.getStaffRole();
        CsvCell ppaId = parser.getPPAID();
        CsvCell gpLocalCode = parser.getGPLocalCode();
        CsvCell gmpCode = parser.getGmpID();
        CsvCurrentState currentState = parser.getCurrentState();

        Record r = new Record(staffId, profileIdCell, orgId, removedDataCell, roleStart,
                roleEnd, roleName, ppaId, gpLocalCode, gmpCode, currentState);
        staffToSave.add(r);

        if (staffToSave.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
            List<Record> copy = new ArrayList<>(staffToSave);
            staffToSave.clear();

            csvHelper.submitToThreadPool(new Task(copy, csvHelper, adminCacheFiler, fhirResourceFiler));
        }
    }


    static class Record {
        private CsvCell staffIdCell;
        private CsvCell profileIdCell;
        private CsvCell orgIdCell;
        private CsvCell removedDataCell;
        private CsvCell roleStartCell;
        private CsvCell roleEndCell;
        private CsvCell roleNameCell;
        private CsvCell ppaIdCell;
        private CsvCell gpLocalCodeCell;
        private CsvCell gmpCodeCell;
        private CsvCurrentState currentState;
        private Date startDate;
        private Date endDate;

        public Record(CsvCell staffIdCell, CsvCell profileIdCell, CsvCell orgIdCell, CsvCell removedDataCell,
                      CsvCell roleStartCell, CsvCell roleEndCell, CsvCell roleNameCell, CsvCell ppaIdCell,
                      CsvCell gpLocalCodeCell, CsvCell gmpCodeCell, CsvCurrentState currentState) throws Exception {
            this.staffIdCell = staffIdCell;
            this.profileIdCell = profileIdCell;
            this.orgIdCell = orgIdCell;
            this.removedDataCell = removedDataCell;
            this.roleStartCell = roleStartCell;
            this.roleEndCell = roleEndCell;
            this.roleNameCell = roleNameCell;
            this.ppaIdCell = ppaIdCell;
            this.gpLocalCodeCell = gpLocalCodeCell;
            this.gmpCodeCell = gmpCodeCell;
            this.currentState = currentState;

            //SimpleDateFormat is not thread safe, so we we must parse the dates out now, while we're
            //still single threaded
            if (roleStartCell != null) {
                this.startDate = roleStartCell.getDate();
            }
            if (roleEndCell != null) {
                this.endDate = roleEndCell.getDate();
            }

        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("record num = " + currentState.getRecordNumber() + ", ");
            sb.append("staffIdCell = " + (staffIdCell != null ? staffIdCell.getString(): null) + ", ");
            sb.append("profileIdCell = " + (profileIdCell != null ? profileIdCell.getString(): null) + ", ");
            sb.append("orgIdCell = " + (orgIdCell != null ? orgIdCell.getString(): null) + ", ");
            sb.append("removedDataCell = " + (removedDataCell != null ? removedDataCell.getString(): null) + ", ");
            sb.append("roleStartCell = " + (roleStartCell != null ? roleStartCell.getString(): null) + ", ");
            sb.append("roleEndCell = " + (roleEndCell != null ? roleEndCell.getString(): null) + ", ");
            sb.append("roleNameCell = " + (roleNameCell != null ? roleNameCell.getString(): null) + ", ");
            sb.append("ppaIdCell = " + (ppaIdCell != null ? ppaIdCell.getString(): null) + ", ");
            sb.append("gpLocalCodeCell = " + (gpLocalCodeCell != null ? gpLocalCodeCell.getString(): null) + ", ");
            sb.append("gmpCodeCell = " + (gmpCodeCell != null ? gmpCodeCell.getString(): null) + ", ");
            return sb.toString();
        }


        public CsvCell getStaffIdCell() {
            return staffIdCell;
        }

        public CsvCell getProfileIdCell() {
            return profileIdCell;
        }

        public CsvCell getOrgIdCell() {
            return orgIdCell;
        }

        public CsvCell getRemovedDataCell() {
            return removedDataCell;
        }

        public CsvCell getRoleStartCell() {
            return roleStartCell;
        }

        public CsvCell getRoleEndCell() {
            return roleEndCell;
        }

        public CsvCell getRoleNameCell() {
            return roleNameCell;
        }

        public CsvCell getPpaIdCell() {
            return ppaIdCell;
        }

        public CsvCell getGpLocalCodeCell() {
            return gpLocalCodeCell;
        }

        public CsvCell getGmpCodeCell() {
            return gmpCodeCell;
        }

        public CsvCurrentState getCurrentState() {
            return currentState;
        }

        public Date getStartDate() {
            return startDate;
        }

        public Date getEndDate() {
            return endDate;
        }
    }

    static class Task implements Callable {

        private final List<Record> staff;
        private final TppCsvHelper csvHelper;
        private final EmisAdminCacheFiler adminCacheFiler;
        private final FhirResourceFiler fhirResourceFiler;

        public Task(List<Record> staff, TppCsvHelper csvHelper, EmisAdminCacheFiler adminCacheFiler, FhirResourceFiler fhirResourceFiler) {
            this.staff = staff;
            this.csvHelper = csvHelper;
            this.adminCacheFiler = adminCacheFiler;
            this.fhirResourceFiler = fhirResourceFiler;
        }


        @Override
        public Object call() throws Exception {

            try {

                //hit the admin resource cache first, for all records at once
                List<String> profileIds = new ArrayList<>();
                for (Record r: staff) {
                    CsvCell profileIdCell = r.getProfileIdCell();
                    profileIds.add(profileIdCell.getString());
                }

                //rather than retrieve the ID mapped instance of the resource, retrieve the resource from the admin resource cache,
                //which gives us a non-ID mapped version to work with. This then lets us easily save our changes back to the admin resource cache
                Map<String, EmisAdminResourceCache> adminCacheResources = adminCacheFiler.getResourcesFromCache(ResourceType.Practitioner, profileIds);

                //now process each record in turn
                for (Record r: staff) {

                    CsvCell profileIdCell = r.getProfileIdCell();

                    PractitionerBuilder practitionerBuilder = null;

                    EmisAdminResourceCache adminCacheResource = adminCacheResources.get(profileIdCell.getString());
                    if (adminCacheResource == null) {
                        practitionerBuilder = new PractitionerBuilder();
                        practitionerBuilder.setId(profileIdCell.getString(), profileIdCell);
                        practitionerBuilder.setActive(true); //only ever set active to true here, as there are several places this can be set to false

                    } else {
                        String json = adminCacheResource.getResourceData();
                        Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(json);
                        ResourceFieldMappingAudit audit = adminCacheResource.getAudit();

                        practitionerBuilder = new PractitionerBuilder(practitioner, audit);
                    }

                    CsvCell removedDataCell = r.getRemovedDataCell();
                    if (removedDataCell != null //null check required because the column isn't always present
                            && removedDataCell.getIntAsBoolean()) {
                        //if the profile has been removed, mark the practitioner as non-active but don't
                        //delete, since data may reference it
                        practitionerBuilder.setActive(false, removedDataCell);

                    } else {

                        //if not removed, always make sure to re-set this to true in case it was previously removed
                        practitionerBuilder.setActive(true, removedDataCell);

                        //since this may be an update to an existing profile, we need to make sure to remove any existing matching instance
                        //from the practitioner resource
                        String profileId = profileIdCell.getString();
                        PractitionerRoleBuilder.removeRoleForId(practitionerBuilder, profileId);

                        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

                        //set the profile ID on the role element, so we can match up when we get updates
                        roleBuilder.setId(profileId, profileIdCell);

                        // This is a candidate for refactoring with the same code in SRStaffMemberTransformer - maybe when I'm more certain of FhirResourceFiler
                        CsvCell orgIdCell = r.getOrgIdCell();
                        if (!orgIdCell.isEmpty()) {
                            Reference organisationReference = csvHelper.createOrganisationReference(orgIdCell.getString());
                            roleBuilder.setRoleManagingOrganisation(organisationReference, orgIdCell);
                        }

                        CsvCell roleStartCell = r.getRoleStartCell();
                        if (!roleStartCell.isEmpty()) {
                            Date startDate = r.getStartDate();
                            roleBuilder.setRoleStartDate(startDate, roleStartCell);
                        }

                        CsvCell roleEndCell = r.getRoleEndCell();
                        if (!roleEndCell.isEmpty()) {
                            Date endDate = r.getEndDate();
                            roleBuilder.setRoleEndDate(endDate, roleEndCell);
                        }

                        CsvCell roleName = r.getRoleNameCell();
                        if (!roleName.isEmpty()) {
                            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
                            codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
                            codeableConceptBuilder.setCodingDisplay(roleName.getString(), roleName);
                        }

                        CsvCell ppaId = r.getPpaIdCell();
                        if (!ppaId.isEmpty()) {
                            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);

                            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
                            identifierBuilder.setValue(ppaId.getString(), ppaId);
                        }

                        CsvCell gpLocalCode = r.getGpLocalCodeCell();
                        if (!gpLocalCode.isEmpty()) {
                            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);

                            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
                            identifierBuilder.setValue(gpLocalCode.getString(), gpLocalCode);
                        }

                        CsvCell gmpCode = r.getGmpCodeCell();
                        if (!gmpCode.isEmpty()) {
                            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);

                            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
                            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
                        }
                    }

                    //see if our staff member cache has updated data from the SRStaffMember file
                    CsvCell staffId = r.getStaffIdCell();
                    csvHelper.getStaffMemberCache().addOrUpdatePractitionerDetails(practitionerBuilder, csvHelper, staffId, profileIdCell);

                    //save back to the admin cache - this is saved to the DB in batches internally, so no need to batch here
                    adminCacheFiler.saveAdminResourceToCache(practitionerBuilder);

                    //if we're not interested in this practitioner, then don't save to the main DB
                    if (csvHelper.getStaffMemberCache().shouldSavePractitioner(practitionerBuilder, csvHelper)) {
                        fhirResourceFiler.saveAdminResource(r.getCurrentState(), practitionerBuilder);
                    }
                }

            } catch (Throwable t) {
                StringBuilder sb = new StringBuilder();
                sb.append("Error saving " + staff.size() + " staff member profile IDs");
                for (Record r: staff) {
                    sb.append(" " + r.getProfileIdCell().getString());
                    *//*sb.append("\r\n");
                    sb.append(r.toString());*//*
                }

                String msg = sb.toString();
                LOG.error(msg, t);
                throw new TransformException(msg, t);
            }

            return null;
        }
    }*/
}
