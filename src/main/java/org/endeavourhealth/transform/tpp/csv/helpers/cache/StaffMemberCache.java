package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppStaffDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMember;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMemberProfile;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class StaffMemberCache {
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberCache.class);

    private Set<Integer> hsChangedProfileIds = ConcurrentHashMap.newKeySet(); //profile IDs that have changed

    private Set<Integer> hsRequiredProfileIds = ConcurrentHashMap.newKeySet(); //profile IDs we know are needed by clinical data
    private Map<String, Set<Integer>> hmRequiredOrgAndStaffIds = new ConcurrentHashMap<>(); //staff and org IDs we know are needed
    private Set<String> hsRequiredOrgIds = ConcurrentHashMap.newKeySet(); //org IDs (without staff ID) we know are needed

    private Map<CacheKey, Integer> hmCachedStaffToProfileIds = null; //start as null so if anything tries to access this cache too early, it'll fail
    private ReentrantLock lock = new ReentrantLock();


    public void addChangedProfileId(CsvCell profileIdCell) {

        Integer profileId = profileIdCell.getInt();
        this.hsChangedProfileIds.add(profileId);
    }

    /**
     * if a clinical (e.g. SREvent) record references a staff ID, we call this fn to log that we definitely want to transform that record
     */
    public void addRequiredStaffId(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) {

        //if no org ID ignore it
        if (organisationDoneAtCell.isEmpty()) {
            return;
        }

        if (TppCsvHelper.isEmptyOrNegative(staffMemberIdCell)) {
            addRequiredOrgIdImpl(organisationDoneAtCell);

        } else {
            addRequiredStaffAndOrgIdImpl(staffMemberIdCell, organisationDoneAtCell);
        }
    }

    private void addRequiredOrgIdImpl(CsvCell organisationDoneAtCell) {
        String orgId = organisationDoneAtCell.getString();
        hsRequiredOrgIds.add(orgId);
    }

    private void addRequiredStaffAndOrgIdImpl(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) {

        Integer staffId = staffMemberIdCell.getInt();
        String orgId = organisationDoneAtCell.getString();

        Set<Integer> staffIds = hmRequiredOrgAndStaffIds.get(orgId);
        if (staffIds == null) {
            //if null, lock and check again, because this will be called from multiple threads
            try {
                lock.lock();

                staffIds = hmRequiredOrgAndStaffIds.get(orgId);
                if (staffIds == null) {
                    //if still null, create the set and add, now that we're safely locked
                    staffIds = ConcurrentHashMap.newKeySet();
                    hmRequiredOrgAndStaffIds.put(orgId, staffIds);
                }

            } finally {
                lock.unlock();
            }
        }

        staffIds.add(staffId);
    }

    /**
     * if a clinical (e.g. SREvent) record references a profile ID, we call this fn to log that we definitely want to transform that record
     */
    public void addRequiredProfileId(CsvCell profileIdCell) {

        if (TppCsvHelper.isEmptyOrNegative(profileIdCell)) {
            return;
        }

        Integer profileId = profileIdCell.getInt();
        hsRequiredProfileIds.add(profileId);
    }


    /**
     * called after the staff and profile staging tables have been updated to create Practitioners
     * from the changed IDs we cached
     */
    public void processChangedStaffMembers(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        processChangedStaffMembersWithProfiles(csvHelper, fhirResourceFiler);
        createDummyPractitionersForUnknownStaff(csvHelper, fhirResourceFiler);

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        //null these to release memory and to cause errors if this fn is called again
        hsChangedProfileIds = null;
        hsRequiredProfileIds = null;
        hmRequiredOrgAndStaffIds = null;
        hsRequiredOrgIds = null;
    }

    /**
     * we need a Practitioner to link data to an organisation even when the StaffMemberID is null, so we
     * create a dummy Practitioner to provide that link
     */
    private void createDummyPractitionersForUnknownStaff(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        LOG.debug("Going to create dummy practitioners for unknown staff");
        int done = 0;

        for (String requiredOrgId: hsRequiredOrgIds) {

            //if we already have created a dummy practitioner for this org ID, we don't need to do it again
            UUID mappedId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Practitioner, requiredOrgId);
            if (mappedId != null) {
                continue;
            }

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder();

            //use the Org ID as the source ID
            practitionerBuilder.setId(requiredOrgId);

            //give a suitable name
            NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.setText("Unknown Staff Member at " + requiredOrgId);

            //give it a role at the organisation
            PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);
            Reference organisationReference = csvHelper.createOrganisationReference(requiredOrgId);
            roleBuilder.setRoleManagingOrganisation(organisationReference);

            //may as well mark that it's not an active staff member
            practitionerBuilder.setActive(false);

            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);

            done ++;
        }

        LOG.debug("Finished creating dummy practitioners, creating " + done + " for " + hsRequiredOrgIds.size() + " IDs");
    }

    private void processChangedStaffMembersWithProfiles(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.debug("Going to process staff members");

        //find the profile IDs we actually want to transform
        Set<Integer> profileIdsToTransform = findProfileIdsToTransform(csvHelper);
        LOG.debug("Going to transform " + profileIdsToTransform.size() + " profiles");

        //then create FHIR practitioners for each one, using threads to speed up
        Set<Integer> batch = new HashSet<>();
        int done = 0;

        for (Integer profileId: profileIdsToTransform) {
            batch.add(profileId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CreatePractitioners(batch, csvHelper, fhirResourceFiler));
                batch.clear();
            }

            done ++;
            if (done % 10000 == 0) {
                LOG.debug("Submitted " + done + " to thread pool");
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CreatePractitioners(batch, csvHelper, fhirResourceFiler));
            batch.clear();
        }

        LOG.debug("Finished after doing " + done);
    }

    private Set<Integer> findProfileIdsToTransform(TppCsvHelper csvHelper) throws Exception {

        Set<Integer> profileIdsToTransform = new HashSet<>();

        //for each CHANGED profile ID, find out if we've previously transformed it or not, then add to TO TRANSFORM list
        LOG.debug("Profiles changed = " + hsChangedProfileIds.size());
        Set<Integer> profileIdsPreviouslyTransformed = findProfileIdsWithMappings(hsChangedProfileIds, csvHelper, true);
        LOG.debug("Profile IDs previously transformed = " + profileIdsPreviouslyTransformed.size());
        profileIdsToTransform.addAll(profileIdsPreviouslyTransformed);

        //for each REQUIRED staff ID, work out its profile ID, then add to required profile IDs list
        convertRequiredStaffToProfileIds(csvHelper);

        //for each REQUIRED profile ID we need to make sure, find if previously transformed - if NOT add to TO TRANSFORM list
        LOG.debug("Profiles required " + hsRequiredProfileIds.size());
        Set<Integer> profileIdsNotPreviouslyTransformed = findProfileIdsWithMappings(hsRequiredProfileIds, csvHelper, false);
        profileIdsToTransform.addAll(profileIdsNotPreviouslyTransformed);

        LOG.debug("Found profiles to transform " + profileIdsToTransform.size());
        return profileIdsToTransform;
    }

    private void convertRequiredStaffToProfileIds(TppCsvHelper csvHelper) throws Exception {

        //populate our cache of staff to profile IDs
        this.hmCachedStaffToProfileIds = new ConcurrentHashMap<>();

        LOG.debug("Required set of orgs = " + hmRequiredOrgAndStaffIds.size());

        for (String orgId: this.hmRequiredOrgAndStaffIds.keySet()) {
            Set<Integer> staffIds = hmRequiredOrgAndStaffIds.get(orgId);
            //LOG.debug("For org " + orgId + " requires staff IDs " + staffIds.size());
            csvHelper.submitToThreadPool(new FindProfileIdsForStaff(orgId, staffIds, hmCachedStaffToProfileIds));
        }

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        //then use the cache just built to populate our required profile IDs
        LOG.debug("Cached profile IDs = " + hmCachedStaffToProfileIds.size());
        LOG.debug("Required profile IDs before = " + hsRequiredProfileIds.size());
        for (CacheKey key: hmCachedStaffToProfileIds.keySet()) {
            Integer profileId = hmCachedStaffToProfileIds.get(key);
            this.hsRequiredProfileIds.add(profileId);
        }
        LOG.debug("Required profile IDs after = " + hsRequiredProfileIds.size());
    }

    private static Set<Integer> findProfileIdsWithMappings(Set<Integer> profileIds, TppCsvHelper csvHelper, boolean findOnesWithMappings) throws Exception {

        Set<Integer> ret = ConcurrentHashMap.newKeySet();

        Set<Integer> batch = new HashSet<>();

        for (Integer profileId: profileIds) {
            batch.add(profileId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId(), findOnesWithMappings));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId(), findOnesWithMappings));
            batch.clear();
        }

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        return ret;
    }


    public Object findProfileIdForStaffMemberAndOrg(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {

        //if we don't have an org ID then we can't do anything
        if (organisationDoneAtCell.isEmpty()) {
            return null;
        }

        Object ret = null;
        if (TppCsvHelper.isEmptyOrNegative(staffMemberIdCell)) {
            ret = findProfileIdForOrgImpl(organisationDoneAtCell);

        } else {
            ret = findProfileIdForStaffMemberAndOrgImpl(staffMemberIdCell, organisationDoneAtCell);
            if (ret == null) {
                //if we can't find a staff profile at the specified org then use the org ID on its own
                //so at least the practitioner reference will be set on whatever clinical data we're transforming
                ret = findProfileIdForOrgImpl(organisationDoneAtCell);
            }
        }

        return ret;
    }

    private String findProfileIdForOrgImpl(CsvCell organisationDoneAtCell) throws Exception {
        //just use the org ID as the source ID
        return organisationDoneAtCell.getString();
    }

    private Integer findProfileIdForStaffMemberAndOrgImpl(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {

        CacheKey key = new CacheKey(staffMemberIdCell, organisationDoneAtCell);
        Integer profileId = hmCachedStaffToProfileIds.get(key);
        if (profileId == null) {
            //normally the pre-transformer for SREvent will ensure we know what staff and profile IDs are needed for the rest
            //of the data, but if we get an amendment to an old data item (e.g. SRImmunisation) then we won't have the SREvent
            //in the latest files, so won't have cached the required staff details. If that happens, just look up and add to the cache
            TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
            String orgId = organisationDoneAtCell.getString();
            Integer staffId = staffMemberIdCell.getInt();
            Set<Integer> staffIds = new HashSet<>();
            staffIds.add(staffId);
            Map<Integer, Integer> map = dal.findProfileIdsForStaffMemberIdsAtOrg(orgId, staffIds);
            if (map.isEmpty()) {
                LOG.warn("NULL profile ID found for staff ID " + staffMemberIdCell.getInt() + " at org " + organisationDoneAtCell.getString());

            } else {
                profileId = map.get(staffId);
                hmCachedStaffToProfileIds.put(key, profileId);
            }
        }
        return profileId;
    }




    private static String getNationalIdTypeIdentifierSystem(String nationalIdType) throws Exception {

        if (nationalIdType.equalsIgnoreCase("GMC")) {
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER;

        } else if (nationalIdType.equalsIgnoreCase("NMC")) {
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_NMC_NUMBER;

        } else if (nationalIdType.equalsIgnoreCase("GDP ID")) { //general dental practitioner
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_GDP_NUMBER;

        } else if (nationalIdType.equalsIgnoreCase("RPSGB")) { //royal pharmaceutical society of GB
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_RPSGB_NUMBER;

        } else if (nationalIdType.equalsIgnoreCase("GPhC")) { //general pharmaceutical council
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_GPhC_NUMBER;

        } else if (nationalIdType.equalsIgnoreCase("HPC") //old name
                || nationalIdType.equalsIgnoreCase("HCPC")) { //new name
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_HPC;

        } else if (nationalIdType.equalsIgnoreCase("PPAID")) {
            return FhirIdentifierUri.IDENTIFIER_SYSTEM_PRESCRIBING_ID;

        } else if (nationalIdType.equalsIgnoreCase("Pathology ID")) {
            return null;

        } else if (nationalIdType.equalsIgnoreCase("Local Person ID")) {
            return null;

        } else if (nationalIdType.equalsIgnoreCase("Other")) {
            return null;

        } else {
            //we've processed 100+ TPP practices now, and the above list seems stable, so don't create more
            //work and just fail hard if we get a new ID we don't understand
            throw new TransformException("Unsupported TPP national ID type [" + nationalIdType + "]");
            /*TransformWarnings.log(LOG, csvHelper, "TPP National ID type {} not mapped", nationalIdType);
            return null;*/
        }
    }

    /**
     * runnable to check if profile ID -> UUID mappings mappings exist and store the IDs that do (or don't)
     */
    private static class CheckForIdMappings implements Callable {

        private Set<Integer> profileIds;
        private Set<Integer> ret;
        private UUID serviceId;
        private boolean findOnesWithMappings;

        public CheckForIdMappings(Set<Integer> profileIds, Set<Integer> ret, UUID serviceId, boolean findOnesWithMappings) {
            this.profileIds = new HashSet<>(profileIds); //create copy because original will be changed
            this.ret = ret;
            this.serviceId = serviceId;
            this.findOnesWithMappings = findOnesWithMappings;
        }

        @Override
        public Object call() throws Exception {

            try {

                //need to create reference objects for each
                Set<Reference> refSet = new HashSet<>();
                for (Integer profileId: profileIds) {
                    Reference ref = ReferenceHelper.createReference(ResourceType.Practitioner, "" + profileId);
                    refSet.add(ref);
                }

                //look up ID mappings for the references
                Map<Reference, UUID> mappings = IdHelper.getEdsResourceIds(serviceId, refSet);

                //add profile IDs for any mappings found to the result set
                for (Reference ref: refSet) {
                    boolean hasMapping = mappings.containsKey(ref);
                    if (findOnesWithMappings == hasMapping) {
                        String refStr = ReferenceHelper.getReferenceId(ref);
                        Integer profileId = Integer.valueOf(refStr);
                        ret.add(profileId);
                    }
                }

            } catch (Throwable t) {
                String msg = "Error looking up ID mappings for profile IDs " + profileIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }

    private static class FindProfileIdsForStaff implements Callable {

        private String orgId;
        private Set<Integer> staffIds;
        private Map<CacheKey, Integer> resultSet;

        public FindProfileIdsForStaff(String orgId, Set<Integer> staffIds, Map<CacheKey, Integer> resultSet) {
            this.orgId = orgId;
            this.staffIds = staffIds;
            this.resultSet = resultSet;
        }

        @Override
        public Object call() throws Exception {

            try {

                TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
                Map<Integer, Integer> hmStaffAndProfileIds = dal.findProfileIdsForStaffMemberIdsAtOrg(orgId, staffIds);
                for (Integer staffId: hmStaffAndProfileIds.keySet()) {
                    Integer profileId = hmStaffAndProfileIds.get(staffId);
                    resultSet.put(new CacheKey(staffId.intValue(), orgId), profileId);
                }

            } catch (Throwable t) {
                String msg = "Error finding profile IDs at org " + orgId + " for staff IDs " + staffIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }

    private static class CacheKey {
        private int staffId;
        private String orgId;

        public CacheKey(CsvCell staffIdCell, CsvCell orgIdCell) {
            this(staffIdCell.getInt().intValue(), orgIdCell.getString());
        }

        public CacheKey(int staffId, String orgId) {
            this.staffId = staffId;
            this.orgId = orgId;
        }

        public int getStaffId() {
            return staffId;
        }

        public String getOrgId() {
            return orgId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (staffId != cacheKey.staffId) return false;
            return orgId.equals(cacheKey.orgId);
        }

        @Override
        public int hashCode() {
            int result = staffId;
            result = 31 * result + orgId.hashCode();
            return result;
        }
    }

    private class CreatePractitioners implements Callable {

        private Set<Integer> profileIds;
        private TppCsvHelper csvHelper;
        private FhirResourceFiler fhirResourceFiler;

        public CreatePractitioners(Set<Integer> profileIds, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) {
            this.profileIds = new HashSet<>(profileIds); //copy because the original will keep changing
            this.csvHelper = csvHelper;
            this.fhirResourceFiler = fhirResourceFiler;
        }


        @Override
        public Object call() throws Exception {

            try {

                TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
                Map<TppStaffMemberProfile, TppStaffMember> map = dal.retrieveRecordsForProfileIds(profileIds);
                for (TppStaffMemberProfile profile: map.keySet()) {
                    TppStaffMember staff = map.get(profile);

                    //audit the file and records we used
                    ResourceFieldMappingAudit audit = new ResourceFieldMappingAudit();
                    audit.auditRecord(staff.getPublishedFileId(), staff.getPublishedFileRecordNumber());
                    audit.auditRecord(profile.getPublishedFileId(), profile.getPublishedFileRecordNumber());

                    PractitionerBuilder practitionerBuilder = new PractitionerBuilder(null, audit);

                    //populate from staff record
                    String fullName = staff.getStaffName();
                    if (!Strings.isNullOrEmpty(fullName)) {
                        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
                        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
                        nameBuilder.setText(fullName);
                    }

                    String username = staff.getUsername();
                    if (!Strings.isNullOrEmpty(username)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
                        identifierBuilder.setValue(username);
                    }

                    String nationalIdType = staff.getNationalIdType();
                    String nationalId = staff.getNationalId();
                    if (!Strings.isNullOrEmpty(nationalIdType)
                            && !Strings.isNullOrEmpty(nationalId)) {

                        String nationalIdTypeSystem = getNationalIdTypeIdentifierSystem(nationalIdType);
                        if (!Strings.isNullOrEmpty(nationalIdTypeSystem)) {
                            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                            identifierBuilder.setSystem(nationalIdTypeSystem);
                            identifierBuilder.setValue(nationalId);
                        }
                    }

                    String smartcardId = staff.getSmartcardId();
                    if (!Strings.isNullOrEmpty(smartcardId)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);
                        identifierBuilder.setValue(smartcardId);
                    }

                    //populate from profile record
                    practitionerBuilder.setId("" + profile.getRowId());


                    PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

                    String orgId = profile.getOrganisationId();
                    if (!Strings.isNullOrEmpty(orgId)) {
                        Reference organisationReference = csvHelper.createOrganisationReference(orgId);
                        roleBuilder.setRoleManagingOrganisation(organisationReference);
                    }

                    Date startDate = profile.getStartDate();
                    if (startDate != null) {
                        roleBuilder.setRoleStartDate(startDate);
                    }

                    Date endDate = profile.getEndDate();
                    if (endDate != null) {
                        roleBuilder.setRoleEndDate(endDate);
                    }

                    //NOTE: the Profile file has a staff role ID, which refers to a separate file, but this
                    //file just contains the same descriptions. We don't get role CODE anywhere in the TPP data.
                    String roleName = profile.getRoleName();
                    if (!Strings.isNullOrEmpty(roleName)) {
                        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
                        codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
                        codeableConceptBuilder.setCodingDisplay(roleName);
                    }

                    String ppaId = profile.getPpaId();
                    if (!Strings.isNullOrEmpty(ppaId)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
                        identifierBuilder.setValue(ppaId);
                    }

                    String gpLocalCode = profile.getGpLocalCode();
                    if (!Strings.isNullOrEmpty(gpLocalCode)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
                        identifierBuilder.setValue(gpLocalCode);
                    }

                    String gmpCode = profile.getGmpId();
                    if (!Strings.isNullOrEmpty(gmpCode)) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
                        identifierBuilder.setValue(gmpCode);
                    }


                    //calculate the active state from the role start and end date but then override and set to false if "removed"
                    practitionerBuilder.calculateActiveFromRoles();

                    boolean removedData = profile.isRemovedData();
                    if (removedData) {
                        practitionerBuilder.setActive(false);
                    }

                    fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
                }

            } catch (Throwable t) {
                String msg = "Error creating practitioners for profile IDs " + profileIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }
}
