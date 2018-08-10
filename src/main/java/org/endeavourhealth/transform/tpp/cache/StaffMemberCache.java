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
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
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

    private HashMap<Long, StaffMemberCacheObj> cache = new HashMap<>();
    private Set<Long> staffProfileIdsProcessed = new HashSet<>();
    private String cachedOdsCode = null;
    private Set<Long> staffProfileIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //this gives us a concurrent hash set
    private Set<Long> staffMemberIdsThatMustBeTransformed = ConcurrentHashMap.newKeySet(); //this gives us a concurrent hash set
    private InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();

    public void addStaffMemberObj(CsvCell staffMemberIdCell, StaffMemberCacheObj obj) {
        Long key = staffMemberIdCell.getLong();
        cache.put(key, obj);
    }

    public StaffMemberCacheObj getStaffMemberObj(CsvCell staffMemberIdCell, CsvCell staffProfileIdCell) {

        //store the ID of the profile that's requesting the staff member
        staffProfileIdsProcessed.add(staffProfileIdCell.getLong());

        Long key = staffMemberIdCell.getLong();
        return cache.get(key);
    }

    public void processRemainingStaffMembers(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        if (cache.isEmpty()) {
            return;
        }

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

        for (Long staffMemberId: cache.keySet()) {
            StaffMemberCacheObj cachedStaff = cache.get(staffMemberId);

            //since each staff member is independent of each other, we can parallelise this
            Task task = new Task(staffMemberId, cachedStaff, fhirResourceFiler, csvHelper, adminCacheFiler);
            csvHelper.submitToThreadPool(task);
        }

        csvHelper.waitUntilThreadPoolIsEmpty();

        //we must save any past practitioners that we didn't save to our EHR DB, but now are referred to
        for (Long profileId: staffProfileIdsThatMustBeTransformed) {

            EmisAdminResourceCache adminCacheResource = adminCacheFiler.getResourceFromCache(ResourceType.Practitioner, "" + profileId);
            if (adminCacheResource == null) {
                throw new TransformException("No admin cache record found for Practitioner " + profileId);
            }

            String json = adminCacheResource.getResourceData();
            Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(json);
            ResourceFieldMappingAudit audit = adminCacheResource.getAudit();

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner, audit);
            fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
        }

        adminCacheFiler.close();
    }

    public static void addOrUpdatePractitionerDetails(PractitionerBuilder practitionerBuilder, StaffMemberCacheObj cachedStaff, TppCsvHelper csvHelper) throws Exception {

        //remove any existing name before adding the new one
        NameBuilder.removeExistingNames(practitionerBuilder);

        CsvCell fullName = cachedStaff.getFullName();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addFullName(fullName.getString(), fullName);

        CsvCell userName = cachedStaff.getUserName();
        if (!userName.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
            identifierBuilder.setValue(userName.getString(), userName);
        }

        CsvCell nationalIdType = cachedStaff.getNationalIdType();
        if (!nationalIdType.isEmpty()) {
            String nationalIdTypeSystem = getNationalIdTypeIdentifierSystem(nationalIdType.getString(), csvHelper);
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

    private static String getNationalIdTypeIdentifierSystem(String nationalIdType, TppCsvHelper csvHelper) throws Exception {

        switch (nationalIdType.toUpperCase()) {
            case "GMC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER;
            case "NMC":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NMC_NUMBER;
            case "GDP ID": //general dental practitioner
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_GDP_NUMBER;
            case "RPSGB": //royal pharmaceutical society of GB
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_RPSGB_NUMBER;
            case "GPhC": //general pharmaceutical council
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_GPhC_NUMBER;
            case "Pathology ID":
                return null;
            case "Local Person ID":
                return null;
            default:
                TransformWarnings.log(LOG, csvHelper, "TPP National ID type {} not mapped", nationalIdType);
                return null;
        }
    }




    public void ensurePractitionerIsTransformedForStaffProfileId(CsvCell staffProfileIdCell, TppCsvHelper csvHelper) throws Exception {

        //if we have an ID->UUID map for for the practitioner, then we've already transformed the practitioner, so are good
        UUID uuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Practitioner, staffProfileIdCell.getString());
        if (uuid != null) {
            return;
        }

        //if we don't have a mapping, then we've never transformed the practitioner, so need to do it now
        this.staffProfileIdsThatMustBeTransformed.add(staffProfileIdCell.getLong());
    }

    public void ensurePractitionerIsTransformedForStaffMemberId(CsvCell staffMemberIdCell, CsvCell profileIdRecordedBy, CsvCell organisationDoneAtCell, TppCsvHelper csvHelper) throws Exception {

        Long staffMemberId = staffMemberIdCell.getLong();

        //if we've already done the below for this staff member, then return out
        if (staffMemberIdsThatMustBeTransformed.contains(staffMemberId)) {
            return;
        }
        staffMemberIdsThatMustBeTransformed.add(staffMemberId);

        //find all the profiles for this staff member
        List<InternalIdMap> mappings = internalIdDal.getSourceId(csvHelper.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, staffMemberIdCell.getString());
        if (mappings.isEmpty()) {
            throw new TransformException("Failed to find any staff profile IDs for staff member ID " + staffMemberIdCell.getString());
        }

        for (InternalIdMap mapping: mappings) {
            String profileIdStr = mapping.getSourceId();
            CsvCell dummyProfileIdCell = CsvCell.factoryDummyWrapper("" + profileIdStr);
            ensurePractitionerIsTransformedForStaffProfileId(dummyProfileIdCell, csvHelper);
        }
    }

    public boolean shouldSavePractitioner(PractitionerBuilder practitionerBuilder, TppCsvHelper csvHelper) throws Exception {

        //this only works on non-ID mapped resources
        if (practitionerBuilder.isIdMapped()) {
            throw new TransformException("Need non ID-mapped resource");
        }

        //if we have an existing ID->UUID map for for the practitioner, then we've already transformed the practitioner, so want to continue
        String profileId = practitionerBuilder.getResourceId();
        UUID uuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Practitioner, profileId);
        if (uuid != null) {
            return true;
        }

        //if we've found a record that refers to this staff member, then we want to transform it
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

        return false;
    }

    class Task implements Callable {

        private Long staffMemberId;
        private StaffMemberCacheObj cachedStaff;
        private FhirResourceFiler fhirResourceFiler;
        private TppCsvHelper csvHelper;
        private EmisAdminCacheFiler adminCacheFiler;

        public Task(Long staffMemberId, StaffMemberCacheObj cachedStaff, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper, EmisAdminCacheFiler adminCacheFiler) {
            this.staffMemberId = staffMemberId;
            this.cachedStaff = cachedStaff;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.adminCacheFiler = adminCacheFiler;
        }

        @Override
        public Object call() throws Exception {

            //get all the known profile IDs for this staff member
            List<InternalIdMap> mappings = internalIdDal.getSourceId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, "" + staffMemberId);
            for (InternalIdMap mapping: mappings) {
                Long profileId = Long.valueOf(mapping.getSourceId());

                //if we've already processed this staff profile because of an updated record in SRStaffMemberProfile then skip it
                if (staffProfileIdsProcessed.contains(profileId)) {
                    continue;
                }

                //retrieve the practitioner from the admin cache, so it's not in its ID mapped state, which
                //allows us to make changes and then save back to the admin cache
                EmisAdminResourceCache adminCacheResource = adminCacheFiler.getResourceFromCache(ResourceType.Practitioner, "" + profileId);
                if (adminCacheResource == null) {
                    continue;
                }

                String json = adminCacheResource.getResourceData();
                Practitioner practitioner = (Practitioner) FhirSerializationHelper.deserializeResource(json);
                ResourceFieldMappingAudit audit = adminCacheResource.getAudit();

                PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner, audit);

                //update the staff member details
                addOrUpdatePractitionerDetails(practitionerBuilder, cachedStaff, csvHelper);
                adminCacheFiler.saveAdminResourceToCache(practitionerBuilder);

                if (shouldSavePractitioner(practitionerBuilder, csvHelper)) {
                    fhirResourceFiler.saveAdminResource(null, practitionerBuilder);
                }
            }
            return null;
        }
    }
}
