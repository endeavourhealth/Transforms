package org.endeavourhealth.transform.tpp.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppStaffDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMember;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppStaffMemberProfile;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
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

    /*private ConcurrentHashMap<Long, StaffMemberCacheObj> cache = new ConcurrentHashMap<>();
    private Set<String> staffProfileIdsProcessed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private String cachedOdsCode = null;
    private Set<Long> staffProfileIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private Set<Long> staffMemberIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();
    private Map<String, Long> staffMemberToProfileMap = new HashMap<>();*/

    private Set<Integer> hsChangedProfileIds = ConcurrentHashMap.newKeySet();
    private Set<Integer> hsRequiredProfileIds = ConcurrentHashMap.newKeySet();
    private Map<String, Set<Integer>> hmRequiredOrgAndStaffIds = new ConcurrentHashMap();
    private Map<CacheKey, Integer> hmCachedStaffToProfileIds = null; //start as null so if anything tries to access this cache too early, it'll fail
    private ReentrantLock lock = new ReentrantLock();


    public void addChangedProfileId(CsvCell profileIdCell, CsvCell staffMemberIdCell, CsvCell orgIdCell) {

        Integer profileId = profileIdCell.getInt();
        this.hsChangedProfileIds.add(profileId);
    }

    /**
     * if a clinical (e.g. SREvent) record references a staff ID, we call this fn to log that we definitely want to transform that record
     */
    public void addRequiredStaffId(CsvCell staffMemberIdCell, CsvCell organisationDoneAtCell) {

        Integer staffId = staffMemberIdCell.getInt();
        String orgId = organisationDoneAtCell.getString();

        Set<Integer> staffIds = hmRequiredOrgAndStaffIds.get(orgId);
        if (staffIds == null) {
            //if null, lock and check again, because this will be called from multiple threads
            try {
                lock.lock();

                staffIds = hmRequiredOrgAndStaffIds.get(staffId);
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

        Integer profileId = profileIdCell.getInt();
        hsRequiredProfileIds.add(profileId);
    }


    /**
     * called after the staff and profile staging tables have been updated to create Practitioners
     * from the changed IDs we cached
     */
    public void processChangedStaffMembers(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

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

        //block until all runnables are done
        csvHelper.waitUntilThreadPoolIsEmpty();

        LOG.debug("Finished after doing " + done);

        //null these to release memory and to cause errors if this fn is called again
        hsChangedProfileIds = null;
        hsRequiredProfileIds = null;
        hmRequiredOrgAndStaffIds = null;
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
            LOG.debug("For org " + orgId + " requires staff IDs " + staffIds.size());
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


    public Integer findProfileIdForStaffMemberAndOrg(CsvCell staffMemberIdCell, CsvCell profileEnteredByCell, CsvCell organisationDoneAtCell) throws Exception {

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



    /*public void addStaffMemberObj(CsvCell staffMemberIdCell, StaffMemberCacheObj obj) {
        Long key = staffMemberIdCell.getLong();
        cache.put(key, obj);
    }

    public void addOrUpdatePractitionerDetails(PractitionerBuilder practitionerBuilder, TppCsvHelper csvHelper, CsvCell staffMemberIdCell, CsvCell profileIdCell) throws Exception {

        //store the ID of the profile that's requesting the staff member - we can't remove from the cache because
        //multiple profile records may refer to the same staff member, so have to keep them cached
        staffProfileIdsProcessed.add(profileIdCell.getString());

        Long key = staffMemberIdCell.getLong();
        StaffMemberCacheObj cachedStaff = cache.get(key);
        if (cachedStaff == null) {
            return;
        }

        addOrUpdatePractitionerDetails(practitionerBuilder, cachedStaff);
    }

    private static void addOrUpdatePractitionerDetails(PractitionerBuilder practitionerBuilder, StaffMemberCacheObj cachedStaff) throws Exception {

        //remove any existing name before adding the new one
        NameBuilder.removeExistingNames(practitionerBuilder);

        CsvCell fullName = cachedStaff.getFullName();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.setText(fullName.getString(), fullName);

        CsvCell userName = cachedStaff.getUserName();
        if (!userName.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
            identifierBuilder.setValue(userName.getString(), userName);
        }

        CsvCell nationalIdType = cachedStaff.getNationalIdType();
        if (!nationalIdType.isEmpty()) {
            String nationalIdTypeSystem = getNationalIdTypeIdentifierSystem(nationalIdType.getString());
            if (!Strings.isNullOrEmpty(nationalIdTypeSystem)) {

                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, nationalIdTypeSystem);

                CsvCell nationalId = cachedStaff.getNationalId();
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(nationalIdTypeSystem);
                identifierBuilder.setValue(nationalId.getString(), nationalId);
            }
        }

        CsvCell smartCardId = cachedStaff.getSmartCardId();
        if (!smartCardId.isEmpty()) {

            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);
            identifierBuilder.setValue(smartCardId.getString(), smartCardId);
        }

        CsvCell obsolete = cachedStaff.getObsolete();
        if (!obsolete.isEmpty() && obsolete.getBoolean()) {
            //note we don't set to active here, since it may have been marked as non-active by the staff profile transformer
            //and we don't want to accidentally make it active again
            practitionerBuilder.setActive(false, obsolete);
        }
    }


    public void processRemainingStaffMembers(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

        processRemainingStaffRecords(csvHelper, fhirResourceFiler, adminCacheFiler);
        savePractitionersFromResourceCache(csvHelper, fhirResourceFiler, adminCacheFiler);

        adminCacheFiler.close();

        //set all fields to null because this object shouldn't be used after this point - if it is, then something is wrong
        cache = null;
        staffProfileIdsProcessed = null;
        cachedOdsCode = null;
        staffProfileIdsThatMustBeTransformed = null;
        staffMemberIdsThatMustBeTransformed = null;
        internalIdDal = null;
    }

    *//**
     * if we have any SR Staff Member records that were updated w/o corresponding SR StaffMemberProfile records
     * then we need to update the DB with those details
     *//*
    private void processRemainingStaffRecords(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, EmisAdminCacheFiler adminCacheFiler) throws Exception {

        if (cache.isEmpty()) {
            return;
        }

        LOG.debug("Saving " + cache.size() + " staff members to admin DB cache");
        int done = 0;

        ProcessStaffMemberRecordsTask task = null;

        for (Long staffMemberId : cache.keySet()) {
            StaffMemberCacheObj cachedStaff = cache.get(staffMemberId);

            if (task == null) {
                task = new ProcessStaffMemberRecordsTask(fhirResourceFiler, csvHelper, adminCacheFiler);
            }
            task.addStaffMember(staffMemberId, cachedStaff);

            if (task.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(task);
                task = null;
            }

            done++;
            if (done % 10000 == 0) {
                LOG.debug("Done " + done);
            }
        }

        if (task != null
                && !task.isEmpty()) {
            csvHelper.submitToThreadPool(task);
        }

        csvHelper.waitUntilThreadPoolIsEmpty();
        LOG.debug("Finished saving " + cache.size() + " staff members to admin DB cache");
    }

    *//**
     * we must save any past practitioners that we didn't save to our EHR DB, but now are referred to by SREvent
     *//*
    private void savePractitionersFromResourceCache(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, EmisAdminCacheFiler adminCacheFiler) throws Exception {

        if (staffProfileIdsThatMustBeTransformed.isEmpty()) {
            return;
        }

        LOG.debug("Saving " + staffProfileIdsThatMustBeTransformed.size() + " staff members to FHIR DB");
        int done = 0;

        CopyFromResourceCacheTask task = null;

        for (Long profileId: staffProfileIdsThatMustBeTransformed) {

            if (task == null) {
                task = new CopyFromResourceCacheTask(fhirResourceFiler, csvHelper, adminCacheFiler);
            }
            task.addProfileId("" + profileId);

            if (task.size() >= TransformConfig.instance().getResourceSaveBatchSize()) {
                csvHelper.submitToThreadPool(task);
                task = null;
            }

            done++;
            if (done % 10000 == 0) {
                LOG.debug("Done " + done);
            }
        }

        if (task != null
                && !task.isEmpty()) {
            csvHelper.submitToThreadPool(task);
        }

        csvHelper.waitUntilThreadPoolIsEmpty();

        LOG.debug("Finished saving " + staffProfileIdsThatMustBeTransformed.size() + " staff members to FHIR DB");
    }



    *//**
     * from the pre-transform for SREvent we cache all the staff profile IDs that we know we'll need
     *//*
    public void ensurePractitionerIsTransformedForStaffProfileId(CsvCell profileIdCell) throws Exception {
        Long profileId = profileIdCell.getLong();
        this.staffProfileIdsThatMustBeTransformed.add(profileId);
    }

    *//**
     * from the pre-transform for SREvent we cache all the staff member IDs that we know we'll need
     *//*
    public void ensurePractitionerIsTransformedForStaffMemberId(CsvCell staffMemberIdCell, TppCsvHelper csvHelper) throws Exception {

        Long staffMemberId = staffMemberIdCell.getLong();

        //if we've already done the below for this staff member, then return out - this cache is only
        //needed to prevent doing the below lookups on the internal ID map table repeatedly
        if (staffMemberIdsThatMustBeTransformed.contains(staffMemberId)) {
            return;
        }
        staffMemberIdsThatMustBeTransformed.add(staffMemberId);

        //find all the profiles for this staff member
        List<InternalIdMap> mappings = internalIdDal.getSourceId(csvHelper.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, staffMemberIdCell.getString());
        if (mappings.isEmpty() && !staffMemberIdCell.isEmpty()) {

            //Initial load missing staff records
          //  throw new TransformException("Failed to find any staff profile IDs for staff member ID " + staffMemberIdCell.getString());
            TransformWarnings.log(LOG, csvHelper,"Unmapped staff record {} " , staffMemberIdCell.getString());
        }

        for (InternalIdMap mapping: mappings) {
            String profileIdStr = mapping.getSourceId();
            CsvCell dummyProfileIdCell = CsvCell.factoryDummyWrapper("" + profileIdStr);
            ensurePractitionerIsTransformedForStaffProfileId(dummyProfileIdCell);
        }
    }

    public boolean shouldSavePractitioner(PractitionerBuilder practitionerBuilder, TppCsvHelper csvHelper) throws Exception {

        //this only works on non-ID mapped resources
        if (practitionerBuilder.isIdMapped()) {
            throw new TransformException("Need non ID-mapped resource");
        }

        //if we've found a record that refers to this staff member, then we want to transform it
        String profileId = practitionerBuilder.getResourceId();
        boolean mustTransform = this.staffProfileIdsThatMustBeTransformed.remove(Long.valueOf(profileId));
        if (mustTransform) {
            return true;
        }

        //if the practitioner is for our own org, then we always want to transform it
        if (this.cachedOdsCode == null) {
            ServiceDalI serviceDal = DalProvider.factoryServiceDal();
            Service service = serviceDal.getById(csvHelper.getServiceId());
            this.cachedOdsCode = service.getLocalId();
        }

        Practitioner practitioner = (Practitioner)practitionerBuilder.getResource();
        if (practitioner.hasPractitionerRole()) {
            Practitioner.PractitionerPractitionerRoleComponent role = practitioner.getPractitionerRole().get(0);
            if (role.hasManagingOrganization()) {
                Reference orgReference = role.getManagingOrganization();
                String orgOdsCode = ReferenceHelper.getReferenceId(orgReference);

                if (orgOdsCode.equalsIgnoreCase(cachedOdsCode)) {
                    return true;
                }
            }
        }

        //if we have an existing ID->UUID map for for the practitioner, then we've already transformed the practitioner, so want to continue
        UUID uuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Practitioner, profileId);
        if (uuid != null) {
            return true;
        }

        return false;
    }



    public Long findProfileIdForStaffMemberAndOrg(CsvCell staffMemberIdCell, CsvCell profileIdRecordedBy, CsvCell organisationDoneAtCell) {

            //Practitioner resources use the profile ID as the source ID, so need to look up an ID for our staff member
            String cacheKey = staffMemberIdCell.getString() + "/" + profileIdRecordedBy.getString() + "/" + organisationDoneAtCell.getString();
            Long profileId = staffMemberToProfileMap.get(cacheKey);
            if (profileId == null) {

                List<InternalIdMap> mappings = internalIdDal.getSourceId(serviceId, InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, staffMemberIdCell.getString());
                if (mappings.isEmpty() && !staffMemberIdCell.isEmpty()) {
                    TransformWarnings.log(LOG, this, "Failed to find any staff profile IDs for staff member ID {}", staffMemberIdCell.getString());
                    return null;
                    //throw new TransformException("Failed to find any staff profile IDs for staff member ID " + staffMemberIdCell.getString());
                }

                //our staff member is likely to have multiple role profiles, so we use the profile ID recorded by and organisation
                //to narrow it down to the correct one, since 99% of the time, the person who recorded the consultation actually did the consultation

                //if one of the profiles for the staff member is the same as recorded the consultation, then that's the one to us
                if (!profileIdRecordedBy.isEmpty()) {
                    for (InternalIdMap mapping : mappings) {
                        if (mapping.getSourceId().equals(profileIdRecordedBy.getString())) {
                            profileId = Long.valueOf(mapping.getSourceId());
                            break;
                        }
                    }
                }

                //if we know the organisation is was done at, we can try to use that to narrow down the profile ID
                if (profileId == null
                        && !organisationDoneAtCell.isEmpty()) {
                    for (InternalIdMap mapping : mappings) {

                        String mappingProfileId = mapping.getSourceId();

                        //note that we don't save practitioners to the EHR database until needed, so we have to use the admin cache
                        //as a source for the UNMAPPED practitioner data
                        EmisTransformDalI dal = DalProvider.factoryEmisTransformDal();
                        EmisAdminResourceCache adminResourceObj = dal.getAdminResource(TppCsvHelper.ADMIN_CACHE_KEY, ResourceType.Practitioner, mappingProfileId);
                        if (adminResourceObj == null) {
                            continue;
                        }

                        //note this practitioner is NOT ID mapped
                        Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(adminResourceObj.getResourceData());
                        if (practitioner.hasPractitionerRole()) {

                            Practitioner.PractitionerPractitionerRoleComponent role = practitioner.getPractitionerRole().get(0);
                            if (role.hasManagingOrganization()) {
                                Reference orgReference = role.getManagingOrganization();
                                String sourceOrgId = ReferenceHelper.getReferenceId(orgReference);
                                if (sourceOrgId.equalsIgnoreCase(organisationDoneAtCell.getString())) {
                                    profileId = Long.valueOf(mapping.getSourceId());
                                    break;
                                }
                            }
                        }
                    }
                }

                //if we still can't work out which profile it is, fall back on using the first
                if (profileId == null) {
                    InternalIdMap mapping = mappings.get(0);
                    profileId = Long.valueOf(mapping.getSourceId());
                }

                staffMemberToProfileMap.put(cacheKey, profileId);
            }

            return profileId;
    }




    class ProcessStaffMemberRecordsTask implements Callable {

        private Map<Long, StaffMemberCacheObj> hmStaffMemberRecordsById = new HashMap<>();
        private FhirResourceFiler fhirResourceFiler;
        private TppCsvHelper csvHelper;
        private EmisAdminCacheFiler adminCacheFiler;

        public ProcessStaffMemberRecordsTask(FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper, EmisAdminCacheFiler adminCacheFiler) {
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.adminCacheFiler = adminCacheFiler;
        }

        public void addStaffMember(Long id, StaffMemberCacheObj staffMemberCacheObj) {
            hmStaffMemberRecordsById.put(id, staffMemberCacheObj);
        }

        public int size() {
            return hmStaffMemberRecordsById.size();
        }

        public boolean isEmpty() {
            return hmStaffMemberRecordsById.isEmpty();
        }

        @Override
        public Object call() throws Exception {

            //find all the profile IDs for our staff members
            Map<Long, List<String>> hmProfilesByStaff = new HashMap<>();
            List<String> profileIdsList = new ArrayList<>();

            for (Long staffId: hmStaffMemberRecordsById.keySet()) {

                List<String> profileIdsForStaff = new ArrayList<>();

                List<InternalIdMap> mappings = internalIdDal.getSourceId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, "" + staffId);
                for (InternalIdMap mapping : mappings) {
                    String profileIdStr = mapping.getSourceId();

                    //if we've already processed this staff profile because of an updated record in SRStaffMemberProfile then skip it
                    if (staffProfileIdsProcessed.contains(profileIdStr)) {
                        continue;
                    }

                    profileIdsForStaff.add(profileIdStr);
                }

                hmProfilesByStaff.put(staffId, profileIdsForStaff);
                profileIdsList.addAll(profileIdsForStaff);
            }

            //if all profiles have already been updated due to SRStaffMemberProfile, then return out
            if (profileIdsList.isEmpty()) {
                return null;
            }

            //hit the resource cache for all profiles for the IDs we just found
            Map<String, EmisAdminResourceCache> hmAdminCacheResources = adminCacheFiler.getResourcesFromCache(ResourceType.Practitioner, profileIdsList);

            //now process the actual data
            for (Long staffId: hmStaffMemberRecordsById.keySet()) {
                StaffMemberCacheObj staffMemberCacheObj = hmStaffMemberRecordsById.get(staffId);
                List<String> profileIdsForStaff = hmProfilesByStaff.get(staffId);

                for (String profileIdStr: profileIdsForStaff) {

                    //retrieve the practitioner from the admin cache, so it's not in its ID mapped state, which
                    //allows us to make changes and then save back to the admin cache
                    EmisAdminResourceCache adminCacheResource = hmAdminCacheResources.get(profileIdStr);
                    if (adminCacheResource == null) {
                        continue;
                    }

                    String json = adminCacheResource.getResourceData();
                    Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(json);
                    ResourceFieldMappingAudit audit = adminCacheResource.getAudit();

                    PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner, audit);

                    //update the staff member details
                    addOrUpdatePractitionerDetails(practitionerBuilder, staffMemberCacheObj);

                    //save back to admin resource cache
                    adminCacheFiler.saveAdminResourceToCache(practitionerBuilder);

                    //save FHIR resource to EHR DB
                    if (shouldSavePractitioner(practitionerBuilder, csvHelper)) {
                        fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
                    }
                }
            }

            return null;
        }
    }

    class CopyFromResourceCacheTask implements Callable {

        private List<String> profileIds = new ArrayList<>();
        private FhirResourceFiler fhirResourceFiler;
        private TppCsvHelper csvHelper;
        private EmisAdminCacheFiler adminCacheFiler;

        public CopyFromResourceCacheTask(FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper, EmisAdminCacheFiler adminCacheFiler) {
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.adminCacheFiler = adminCacheFiler;
        }

        public int size() {
            return profileIds.size();
        }

        public boolean isEmpty() {
            return profileIds.isEmpty();
        }

        public void addProfileId(String profileId) {
            profileIds.add(profileId);
        }

        @Override
        public Object call() throws Exception {

            //work out which profile IDs have already been saved to the EHR DB
            List<String> profileIdsToCopy = new ArrayList<>();

            for (String profileId: profileIds) {

                //if we have an ID->UUID map for for the practitioner, then we've already transformed the practitioner, so are good
                UUID uuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Practitioner, profileId);
                if (uuid != null) {
                    continue;
                }

                profileIdsToCopy.add(profileId);
            }

            if (profileIdsToCopy.isEmpty()) {
                return null;
            }

            //hit the admin cache DB for all the profiles we know we'll need to copy
            Map<String, EmisAdminResourceCache> hmAdminCacheResources = adminCacheFiler.getResourcesFromCache(ResourceType.Practitioner, profileIdsToCopy);

            //then save each profile to the EHR DB
            for (String profileId: profileIdsToCopy) {

                EmisAdminResourceCache adminCacheResource = hmAdminCacheResources.get(profileId);
                if (adminCacheResource == null) {
                    LOG.error("No admin cache record found for Practitioner " + profileId);
                    continue;
                }

                String json = adminCacheResource.getResourceData();
                Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(json);
                ResourceFieldMappingAudit audit = adminCacheResource.getAudit();

                PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner, audit);
                fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
            }

            return null;
        }
    }*/


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
