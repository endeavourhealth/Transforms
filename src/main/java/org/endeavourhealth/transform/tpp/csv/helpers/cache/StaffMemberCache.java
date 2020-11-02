package org.endeavourhealth.transform.tpp.csv.helpers.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
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

public class StaffMemberCache {
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberCache.class);

    private Set<Integer> hsChangedProfileIds = ConcurrentHashMap.newKeySet(); //profile IDs that have changed in the extract we're processing

    private Map<Integer, Boolean> hmRequiredProfileIds = new HashMap<>(); //profile IDs we need to create practitioners for
    private Map<String, Boolean> hmRequiredOrgIds = new HashMap<>(); //org IDs we need to create practitioners for

    private Map<StaffAndOrgCacheKey, Integer> hmCachedStaffToProfileIds = new HashMap<>(); //cached lookup of staff and org to profile ID

    /**
     * called during the SRStaffMemberProfile transformer to log which profile IDs have changed
     */
    public void addChangedProfileId(CsvCell profileIdCell) {

        Integer profileId = profileIdCell.getInt();
        this.hsChangedProfileIds.add(profileId);
    }

    /**
     * if a clinical (e.g. SREvent) record references a staff ID, we call this fn to log that we definitely want to transform that record
     */
    /*public void addRequiredStaffId(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) {

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

    *//**
     * if a clinical (e.g. SREvent) record references a profile ID, we call this fn to log that we definitely want to transform that record
     *//*
    public void addRequiredProfileId(CsvCell profileIdCell) {

        if (TppCsvHelper.isEmptyOrNegative(profileIdCell)) {
            return;
        }

        Integer profileId = profileIdCell.getInt();
        hsRequiredProfileIds.add(profileId);
    }*/


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
        //these are used for validation after saving all the staff, so don't null them
        /*hsRequiredProfileIds = null;
        hmRequiredOrgAndStaffIds = null;
        hsRequiredOrgIds = null;*/
    }

    /**
     * we need a Practitioner to link data to an organisation even when the StaffMemberID is null, so we
     * create a dummy Practitioner to provide that link
     */
    private void createDummyPractitionersForUnknownStaff(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        LOG.debug("Going to create dummy practitioners for unknown staff");
        int done = 0;

        for (String requiredOrgId: hmRequiredOrgIds.keySet()) {

            //if we already have created a dummy practitioner for this org ID, we don't need to do it again
            Boolean required = hmRequiredOrgIds.get(requiredOrgId);
            if (!required.booleanValue()) {
                continue;
            }

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder();

            //use the Org ID as the source ID
            practitionerBuilder.setId(requiredOrgId);

            String nameStr = "Unknown Staff Member at " + requiredOrgId;

            //if we can get something useful from ODS then do so
            OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(requiredOrgId);
            if (odsOrg != null) {
                nameStr += " (" + odsOrg.getOrganisationName() + ")";
            }

            //give a suitable name
            NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
            nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
            nameBuilder.setText(nameStr);

            //give it a role at the organisation
            PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);
            Reference organisationReference = csvHelper.createOrganisationReference(requiredOrgId);
            roleBuilder.setRoleManagingOrganisation(organisationReference);

            //may as well mark that it's not an active staff member
            practitionerBuilder.setActive(false);

            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);

            done ++;
        }

        LOG.debug("Finished creating dummy practitioners, creating " + done + " for " + hmRequiredOrgIds.size() + " IDs");
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
        Set<Integer> profileIdsPreviouslyTransformed = findProfileIdsWithMappings(hsChangedProfileIds, csvHelper);
        LOG.debug("Profile IDs previously transformed = " + profileIdsPreviouslyTransformed.size());
        profileIdsToTransform.addAll(profileIdsPreviouslyTransformed);

        //for each REQUIRED profile ID add to the set if we've NOT previously transformed it (i.e. it actually is required)
        LOG.debug("Profiles required " + hmRequiredProfileIds.size());
        for (Integer profileId: hmRequiredProfileIds.keySet()) {
            Boolean required = hmRequiredProfileIds.get(profileId);
            if (required.booleanValue()) {
                profileIdsToTransform.add(profileId);
            }
        }

        LOG.debug("Found profiles to transform " + profileIdsToTransform.size());
        return profileIdsToTransform;
    }


    /*private Set<Integer> findProfileIdsToTransform(TppCsvHelper csvHelper) throws Exception {

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
        for (StaffAndOrgCacheKey key: hmCachedStaffToProfileIds.keySet()) {
            Integer profileId = hmCachedStaffToProfileIds.get(key);
            this.hsRequiredProfileIds.add(profileId);
        }
        LOG.debug("Required profile IDs after = " + hsRequiredProfileIds.size());
    }*/

    private static Set<Integer> findProfileIdsWithMappings(Set<Integer> profileIds, TppCsvHelper csvHelper) throws Exception {

        Set<Integer> ret = ConcurrentHashMap.newKeySet();

        Set<Integer> batch = new HashSet<>();

        for (Integer profileId: profileIds) {
            batch.add(profileId);
            if (batch.size() > TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId()));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            csvHelper.submitToThreadPool(new CheckForIdMappings(batch, ret, csvHelper.getServiceId()));
            batch.clear();
        }

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        return ret;
    }

    /**
     * returns a local ID for the given profile ID (which is the profile ID itself)
     */
    public Object findProfileId(UUID serviceId, CsvCell profileIdCell) throws Exception {

        if (TppCsvHelper.isEmptyOrNegative(profileIdCell)) {
            return null;
        }

        //work out if this profile ID is one we'll need to transform into a FHIR Practitioner in case we've ignored it to date
        calculateIfProfileIdIsRequired(serviceId, profileIdCell.getInt());

        //validate that the profile was added to the "required" collections by the pre-transformer. It's critical
        //that the pre-transforms register these required staff because otherwise the FHIR Practitioners won't be created.
        //validateIsRequiredProfile(profileIdCell);

        return profileIdCell.getString();
    }

    private void calculateIfProfileIdIsRequired(UUID serviceId, Integer profileId) throws Exception {

        if (!hmRequiredProfileIds.containsKey(profileId)) {

            //work out if we've ignored it before by simply looking to see if there's an ID for it
            boolean exists = practitionerExistsForSourceId(serviceId, "" + profileId);
            Boolean required = Boolean.valueOf(!exists);
            hmRequiredProfileIds.put(profileId, required);
        }
    }

    private boolean practitionerExistsForSourceId(UUID serviceId, String sourceId) throws Exception {

        //map the local ID to the UUID
        UUID mappedUuid = IdHelper.getEdsResourceId(serviceId, ResourceType.Practitioner, "" + sourceId);
        if (mappedUuid == null) {
            //if not UUID mapping exists, then it definitely doesn't exist
            return false;
        }

        //check the FHIR database
        //we can't just base this on whether a source->UUID mapping exists, since if a transform fails, it may
        //have created the mappings but not yet saved the FHIR resources
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        Long checksum = resourceDal.getResourceChecksum(serviceId, ResourceType.Practitioner.toString(), mappedUuid);
        if (checksum == null) {
            //if not in the FHIR DB, then it definitely doesn't exist
            return false;
        }

        return true;
    }

    /*private void validateIsRequiredProfile(CsvCell profileIdCell) throws Exception {
        Integer profileId = profileIdCell.getInt();
        if (!hsRequiredProfileIds.contains(profileId)) {
            throw new Exception("Profile ID " + profileId + " not registered as required");
        }
    }*/

    /**
     * returns a local ID for the given staff member ID and org ID.
     * If the staff member ID is > 0 then the returned object will be a suitable profile ID at the given org
     * If the staff member ID is <=0 then the returned object will be the org ID itself
     */
    public Object findProfileIdForStaffMemberAndOrg(UUID serviceId, CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {

        //if we don't have an org ID then we can't do anything
        if (organisationDoneAtCell.isEmpty()) {
            return null;
        }

        //validate that the staff member and org we added to the "required" collections by the pre-transformer. It's critical
        //that the pre-transforms register these required staff because otherwise the FHIR Practitioners won't be created.
        //validateIsRequiredStaffMemberAndOrg(staffMemberIdCell, organisationDoneAtCell);

        Object ret = null;
        if (TppCsvHelper.isEmptyOrNegative(staffMemberIdCell)) {

            ret = findProfileIdForOrgImpl(serviceId, organisationDoneAtCell);

        } else {
            ret = findProfileIdForStaffMemberAndOrgImpl(serviceId, staffMemberIdCell, organisationDoneAtCell);
            if (ret == null) {
                //if we can't find a staff profile at the specified org then use the org ID on its own
                //so at least the practitioner reference will be set on whatever clinical data we're transforming
                ret = findProfileIdForOrgImpl(serviceId, organisationDoneAtCell);
            }
        }

        return ret;
    }

    /*private void validateIsRequiredStaffMemberAndOrg(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {

        if (TppCsvHelper.isEmptyOrNegative(staffMemberIdCell)) {
            String orgId = organisationDoneAtCell.getString();
            if (!hsRequiredOrgIds.contains(orgId)) {
                throw new Exception("Org ID " + orgId + " not registered as required");
            }

        } else {
            Integer staffId = staffMemberIdCell.getInt();
            String orgId = organisationDoneAtCell.getString();

            Set<Integer> staffIds = hmRequiredOrgAndStaffIds.get(orgId);
            if (staffIds == null
                    || staffIds.contains(staffId)) {
                throw new Exception("Org ID " + orgId + " and Staff ID " + staffId + " not registered as required");
            }
        }
    }*/

    private String findProfileIdForOrgImpl(UUID serviceId, CsvCell orgIdCell) throws Exception {

        //work out if this org ID is one we'll need to transform into a FHIR Practitioner in case we've ignored it to date
        String orgId = orgIdCell.getString();
        if (!hmRequiredOrgIds.containsKey(orgId)) {

            //work out if we've ignored it before by simply looking to see if there's an ID for it
            boolean exists = practitionerExistsForSourceId(serviceId, orgId);
            Boolean required = Boolean.valueOf(!exists);
            hmRequiredOrgIds.put(orgId, required);
        }

        //just use the org ID as the source ID
        return orgId;
    }

    private Integer findProfileIdForStaffMemberAndOrgImpl(UUID serviceId, CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) throws Exception {

        StaffAndOrgCacheKey key = new StaffAndOrgCacheKey(staffMemberIdCell, organisationDoneAtCell);
        Integer profileId = hmCachedStaffToProfileIds.get(key);
        if (profileId == null) {

            //we first need to convert the staff and org IDs to a profile ID
            TppStaffDalI dal = DalProvider.factoryTppStaffMemberDal();
            String orgId = organisationDoneAtCell.getString();
            Integer staffId = staffMemberIdCell.getInt();
            Set<Integer> staffIds = new HashSet<>();
            staffIds.add(staffId);
            Map<Integer, Integer> map = dal.findProfileIdsForStaffMemberIdsAtOrg(orgId, staffIds);

            //there is some genuine bad data where we have a staff and org combination that doesn't match to a profile
            profileId = map.get(staffId);
            if (profileId == null) {
                LOG.warn("NULL profile ID found for staff ID " + staffMemberIdCell.getInt() + " at org " + organisationDoneAtCell.getString());
                return null;
            }

            //and we also need to work out if we need to create a FHIR practitioner for this profile
            calculateIfProfileIdIsRequired(serviceId, profileId);

            //cache the lookup since we'll most likely have more for this staff member and org
            hmCachedStaffToProfileIds.put(key, profileId);
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

        public CheckForIdMappings(Set<Integer> profileIds, Set<Integer> ret, UUID serviceId) {
            this.profileIds = new HashSet<>(profileIds); //create copy because original will be changed
            this.ret = ret;
            this.serviceId = serviceId;
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
                    if (hasMapping) {
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

    /*private static class FindProfileIdsForStaff implements Callable {

        private String orgId;
        private Set<Integer> staffIds;
        private Map<StaffAndOrgCacheKey, Integer> resultSet;

        public FindProfileIdsForStaff(String orgId, Set<Integer> staffIds, Map<StaffAndOrgCacheKey, Integer> resultSet) {
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
                    resultSet.put(new StaffAndOrgCacheKey(staffId.intValue(), orgId), profileId);
                }

            } catch (Throwable t) {
                String msg = "Error finding profile IDs at org " + orgId + " for staff IDs " + staffIds;
                throw new Exception(msg, t);
            }

            return null;
        }
    }*/

    private static class StaffAndOrgCacheKey {
        private int staffId;
        private String orgId;

        public StaffAndOrgCacheKey(CsvCell staffIdCell, CsvCell orgIdCell) {
            this(staffIdCell.getInt().intValue(), orgIdCell.getString());
        }

        public StaffAndOrgCacheKey(int staffId, String orgId) {
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

            StaffAndOrgCacheKey cacheKey = (StaffAndOrgCacheKey) o;

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
