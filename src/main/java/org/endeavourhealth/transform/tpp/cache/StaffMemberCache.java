package org.endeavourhealth.transform.tpp.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberCacheObj;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StaffMemberCache {
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberCache.class);

    private HashMap<Long, StaffMemberCacheObj> cache = new HashMap<>();
    private Set<Long> staffProfileIdsProcessed = new HashSet<>();

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

        InternalIdDalI internalIdDal = DalProvider.factoryInternalIdDal();

        for (Long staffMemberId: cache.keySet()) {
            StaffMemberCacheObj cachedStaff = cache.get(staffMemberId);

            //get all the known profile IDs for this staff member
            List<InternalIdMap> mappings = internalIdDal.getSourceId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, "" + staffMemberId);
            for (InternalIdMap mapping: mappings) {
                Long profileId = Long.valueOf(mapping.getSourceId());

                //if we've already processed this staff profile because of an updated record in SRStaffMemberProfile then skip it
                if (staffProfileIdsProcessed.contains(profileId)) {
                    continue;
                }

                Practitioner practitioner = (Practitioner)csvHelper.retrieveResource("" + profileId, ResourceType.Practitioner);

                //if we've not transformed this practitioner before, because they're not relevant to our data, then skip it
                if (practitioner == null) {
                    continue;
                }

                PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner);

                //update the staff member details
                addOrUpdatePractitionerDetails(practitionerBuilder, cachedStaff, csvHelper);

                fhirResourceFiler.saveAdminResource(null, false, practitionerBuilder);
            }
        }
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

            default:
                TransformWarnings.log(LOG, csvHelper, "TPP National ID type {} not mapped", nationalIdType);
                return null;
        }
    }
}
