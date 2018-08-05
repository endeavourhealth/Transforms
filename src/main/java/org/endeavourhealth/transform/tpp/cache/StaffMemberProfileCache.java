package org.endeavourhealth.transform.tpp.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberProfilePojo;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

// A simple HashMap with index key and a pojo class as a temporary cache
public class StaffMemberProfileCache {
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberProfileCache.class);

    private HashMap<Long, List<StaffMemberProfilePojo>> staffMemberProfileByStaffId = new HashMap<>();

    public void addStaffPojo(CsvCell staffMemberIdCell, StaffMemberProfilePojo pojo) {
        Long key = staffMemberIdCell.getLong();

        List<StaffMemberProfilePojo> pojoList = staffMemberProfileByStaffId.get(key);
        if (pojoList == null) {
            pojoList = new ArrayList<>();
            staffMemberProfileByStaffId.put(key, pojoList);
        }
        pojoList.add(pojo);
    }

    public List<StaffMemberProfilePojo> getAndRemoveStaffMemberProfilePojo(CsvCell staffMemberIdCell) {
        Long key = staffMemberIdCell.getLong();
        return staffMemberProfileByStaffId.remove(key);
    }

    /*public static void removeStaffPojo(StaffMemberProfilePojo pojo) {
        staffMemberProfileByStaffId.remove(pojo.getIDStaffMember());
    }*/

    /*public static boolean containsStaffId(Long staffId) {
        return (staffMemberProfileByStaffId.containsKey(staffId));
    }*/

    public int size() {
        return staffMemberProfileByStaffId.size();
    }


    public void fileRemainder(TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        // For all remaining StaffMemberProfile records create new Practitioners via
        // PractionerBuilder so these records aren't lost. Presumably the staff records are somewhere in the
        // incoming files.
        for (Long staffMemberId : staffMemberProfileByStaffId.keySet()) {
            List<StaffMemberProfilePojo> pojoList = staffMemberProfileByStaffId.get(staffMemberId);

            //retrieve the existing practitioner from the DB
            Practitioner practitioner = (Practitioner) csvHelper.retrieveResource("" + staffMemberId, ResourceType.Practitioner);
            if (practitioner == null) {
                //if the practitioner has been deleted or something, we'll have null here
                continue;
            }

            PractitionerBuilder practitionerBuilder = new PractitionerBuilder(practitioner);

            for (StaffMemberProfilePojo pojo : pojoList) {
                addOrReplaceProfileOnPractitioner(practitionerBuilder, pojo, csvHelper);
            }

            // We know we need to map Ids as we just built this from local values
            fhirResourceFiler.saveAdminResource(null, false, practitionerBuilder);
        }
        staffMemberProfileByStaffId.clear();
    }

    public static void addOrReplaceProfileOnPractitioner(PractitionerBuilder practitionerBuilder, StaffMemberProfilePojo profileCache, TppCsvHelper csvHelper) throws Exception {

        //since this may be an update to an existing profile, we need to make sure to remove any existing matching instance
        //from the practitioner resource
        CsvCell profileIdCell = profileCache.getStaffMemberProfileIdCell();
        String profileId = profileIdCell.getString();
        PractitionerRoleBuilder.removeRoleForId(practitionerBuilder, profileId);

        if (profileCache.isDeleted()) {
            return;
        }

        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

        //set the profile ID on the role element, so we can match up when we get updates
        roleBuilder.setId(profileId, profileIdCell);

        // This is a candidate for refactoring with the same code in SRStaffMemberTransformer - maybe when I'm more certain of FhirResourceFiler
        String orgId = profileCache.getIdOrganisation();
        if (!Strings.isNullOrEmpty(orgId)) {
            Reference organisationReference = csvHelper.createOrganisationReference(orgId);

            //this function is used for adding profiles to new and existing practitioners, so we need to convert if already mapped
            if (practitionerBuilder.isIdMapped()) {
                organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, csvHelper);
            }

            roleBuilder.setRoleManagingOrganisation(organisationReference);
        }

        Date start = profileCache.getDateEmploymentStart();
        if (start != null) {
            roleBuilder.setRoleStartDate(start);
        }

        Date end = profileCache.getDateEmploymentEnd();
        if (end != null) {
            roleBuilder.setRoleEndDate(end);
        }

        String roleDesc = profileCache.getStaffRole();
        if (!Strings.isNullOrEmpty(roleDesc)) {
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
            codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
            codeableConceptBuilder.setCodingDisplay(roleDesc);
        }

        String ppaid = profileCache.getPpaid();
        if (!Strings.isNullOrEmpty(ppaid)) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
            identifierBuilder.setValue(ppaid);
        }

        String localCode = profileCache.getGpLocalCode();
        if (!Strings.isNullOrEmpty(localCode)) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
            identifierBuilder.setValue(localCode);
        }

        String gmpid = profileCache.getGmpId();
        if (!Strings.isNullOrEmpty(gmpid)) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpid);
        }
    }
}


