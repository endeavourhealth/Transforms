package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.transforms.staff.StaffMemberProfilePojo;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


public class StaffMemberProfileCache {
    // A simple HashMap with index key and a pojo class as a temporary cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffMemberProfileCache.class);

    private static HashMap<Long, List<StaffMemberProfilePojo>> StaffMemberProfileByStaffId = new HashMap<>();

    public static void addStaffPojo(StaffMemberProfilePojo pojo) {
        Long key = pojo.getIDStaffMember();
        if (StaffMemberProfileByStaffId.containsKey(key)) {
            StaffMemberProfileByStaffId.get(key).add(pojo);
        } else {
            List<StaffMemberProfilePojo> pojoList = new ArrayList<StaffMemberProfilePojo>();
            pojoList.add(pojo);
            StaffMemberProfileByStaffId.put(key, pojoList);
        }
    }

    public static List<StaffMemberProfilePojo> getStaffMemberProfilePojo(Long pojoKey) {
        return StaffMemberProfileByStaffId.get(pojoKey);
    }

    public static void removeStaffPojo(StaffMemberProfilePojo pojo) {
        StaffMemberProfileByStaffId.remove(pojo.getIDStaffMember());
    }

    public static boolean containsStaffId(Long staffId) {
        return (StaffMemberProfileByStaffId.containsKey(staffId));
    }

    public static int size() {
        return StaffMemberProfileByStaffId.size();
    }

    public static void clear() {
        LOG.info("Staff member profile cache still has " + size() + " records. Creating Practitioners. ");
        StaffMemberProfileByStaffId.clear();
    }

    public static void fileRemainder(TppCsvHelper  csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception{
        // For all remaining StaffMemberProfile records create new Practitioners via
        // PractionerBuilder so these records aren't lost. Presumably the staff records are somewhere in the
        // incoming files.
        for (Long key : StaffMemberProfileByStaffId.keySet()) {
            List<StaffMemberProfilePojo> pojoList = StaffMemberProfileByStaffId.get(key);
            PractitionerBuilder practitionerBuilder = new PractitionerBuilder();
            String staffMemberId  = pojoList.get(0).getIDStaffMember().toString();
            practitionerBuilder.setId(staffMemberId);
            for (StaffMemberProfilePojo pojo : pojoList) {
                csvHelper.saveInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                        pojo.getIDStaffMemberProfileRole(), staffMemberId);
                CsvCell profileCell = pojo.getRowIdentifier();
                PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);
                // This is a candidate for refactoring with the same code in SRStaffMemberTransformer - maybe when I'm more certain of FhirResourceFiler
                if (pojo.getIDOrganisation() != null) {
                    String orgId = pojo.getIDOrganisation();
                    if (!orgId.isEmpty()) { //shouldn't really happen, but there are a small number, so leave them without an org reference
                        Reference organisationReference = csvHelper.createOrganisationReference(orgId);
                        if (practitionerBuilder.isIdMapped()) {
                            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference,fhirResourceFiler);
                        }
                        roleBuilder.setRoleManagingOrganisation(organisationReference, profileCell);
                    }
                }

                if (pojo.getDateEmploymentStart() !=null) {
                    Date roleStart = pojo.getDateEmploymentStart();
                    if (roleStart != null) {
                        roleBuilder.setRoleStartDate(roleStart, profileCell);
                    }
                }

                if (pojo.getDateEmploymentEnd() != null) {
                    Date roleEnd = pojo.getDateEmploymentEnd();
                    if (roleEnd != null) {
                        roleBuilder.setRoleEndDate(roleEnd, profileCell);
                    }
                }

                if (pojo.getStaffRole() != null) {
                    String roleName = pojo.getStaffRole();
                    if (!roleName.isEmpty()) {
                        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
                        codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
                        codeableConceptBuilder.setCodingDisplay(roleName, profileCell);
                    }
                }

                if (pojo.getPPAID() != null) {
                    String ppaId = pojo.getPPAID();
                    if (!ppaId.isEmpty()) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
                        identifierBuilder.setValue(ppaId, profileCell);
                    }
                }

                if (pojo.getGPLocalCode() != null) {
                    String gpLocalCode = pojo.getGPLocalCode();
                    if (!gpLocalCode.isEmpty()) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
                        identifierBuilder.setValue(gpLocalCode,profileCell);
                    }
                }

                if (pojo.getGmpID() != null) {
                    String gmpCode = pojo.getGmpID();
                    if (!gmpCode.isEmpty()) {
                        IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
                        identifierBuilder.setValue(gmpCode, profileCell);
                    }
                }

                // We know we need to map Ids as we just built this from local values
                fhirResourceFiler.saveAdminResource(pojo.getParserState(),true, practitionerBuilder);


            }
        }
        StaffMemberProfileByStaffId.clear();
    }
}

