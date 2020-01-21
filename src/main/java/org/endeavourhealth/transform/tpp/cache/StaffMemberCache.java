package org.endeavourhealth.transform.tpp.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisAdminResourceCache;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberCacheObj;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class StaffMemberCache {
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberCache.class);

    private ConcurrentHashMap<Long, StaffMemberCacheObj> cache = new ConcurrentHashMap<>();
    private Set<String> staffProfileIdsProcessed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private String cachedOdsCode = null;
    private Set<Long> staffProfileIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private Set<Long> staffMemberIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //gives us a concurrent hash set
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();

    public void addStaffMemberObj(CsvCell staffMemberIdCell, StaffMemberCacheObj obj) {
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

    /**
     * if we have any SR Staff Member records that were updated w/o corresponding SR StaffMemberProfile records
     * then we need to update the DB with those details
     */
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

    /**
     * we must save any past practitioners that we didn't save to our EHR DB, but now are referred to by SREvent
     */
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
     * from the pre-transform for SREvent we cache all the staff profile IDs that we know we'll need
     */
    public void ensurePractitionerIsTransformedForStaffProfileId(CsvCell profileIdCell) throws Exception {
        Long profileId = profileIdCell.getLong();
        this.staffProfileIdsThatMustBeTransformed.add(profileId);
    }

    /**
     * from the pre-transform for SREvent we cache all the staff member IDs that we know we'll need
     */
    public void ensurePractitionerIsTransformedForStaffMemberId(CsvCell staffMemberIdCell, TppCsvHelper csvHelper) throws Exception {

        Long staffMemberId = staffMemberIdCell.getLong();

        //if we've already done the below for this staff member, then return out - this cache is only
        //needed to prevent doing the below lookups on the internal ID map table repeatedly
        if (staffMemberIdsThatMustBeTransformed.contains(staffMemberId)) {
            return;
        }
        staffMemberIdsThatMustBeTransformed.add(staffMemberId);

//TODO - add cache for the below mappings to TppCsvHelper

        //find all the profiles for this staff member
        List<InternalIdMap> mappings = internalIdDal.getSourceId(csvHelper.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, staffMemberIdCell.getString());
        if (mappings.isEmpty() && !staffMemberIdCell.isEmpty()) {
            //TODO Restore exception when we've finished the initial load for Redbridge group.
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
    }
}
