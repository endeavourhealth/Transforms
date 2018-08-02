package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.StaffMemberProfileCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMember;
import org.hl7.fhir.instance.model.HumanName;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class SRStaffMemberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberTransformer.class);
    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMember.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRStaffMember) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
        // Check cache
        if (StaffMemberProfileCache.size() > 0) {
            LOG.error("Staff cache should be empty but has : " + StaffMemberProfileCache.size());
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRStaffMember parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell staffMemberId = parser.getRowIdentifier();
        PractitionerBuilder practitionerBuilder = new PractitionerBuilder();
        practitionerBuilder.setId(staffMemberId.getString(), staffMemberId);

        CsvCell fullName = parser.getStaffName();
        NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addFullName(fullName.getString(), fullName);

        CsvCell userName = parser.getStaffUserName();
        if (!userName.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_USERNAME);
            identifierBuilder.setValue(userName.getString(), userName);
        }

        CsvCell nationalIdType = parser.getNationalIdType();
        if (!nationalIdType.isEmpty()) {
            String nationalIdTypeSystem = getNationalIdTypeIdentifierSystem(nationalIdType.toString());
            if (!Strings.isNullOrEmpty(nationalIdTypeSystem)) {
                CsvCell nationalId = parser.getIDNational();
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(nationalIdTypeSystem);
                identifierBuilder.setValue(nationalId.getString(), nationalId);
            }
        }

        CsvCell smartCardId = parser.getIDSmartCard();
        if (!smartCardId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_SMARTCARD_ID);
            identifierBuilder.setValue(smartCardId.getString(), smartCardId);
        }

        CsvCell obsolete = parser.getObsolete();
        if (!obsolete.isEmpty()) {

            Boolean isActive = !obsolete.getBoolean();
            practitionerBuilder.setActive(isActive, obsolete);
        } else {

            practitionerBuilder.setActive(true, obsolete);
        }
        // Get cached StaffMemberProfile records
        List<StaffMemberProfilePojo> pojoList = StaffMemberProfileCache.getAndRemoveStaffMemberProfilePojo(staffMemberId.getLong());
        if (pojoList != null) {
            for (StaffMemberProfilePojo  pojo : pojoList) {
                CsvCell profileCell = pojo.getRowIdentifier();

                // create the internal link between staff member role and staff member
                //moved to happen in the staff profile transformer
                /*csvHelper.saveInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                       pojo.getIDStaffMemberProfileRole(), staffMemberId.getString());*/

                PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

                if (pojo.getIDOrganisation() != null) {
                    String orgId = pojo.getIDOrganisation();
                    if (!orgId.isEmpty()) { //shouldn't really happen, but there are a small number, so leave them without an org reference
                        Reference organisationReference = csvHelper.createOrganisationReference(orgId);
                        //our practitioner builder is never ID mapped
                        /*if (practitionerBuilder.isIdMapped()) {
                            organisationReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference,fhirResourceFiler);
                        }*/
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
                        identifierBuilder.setValue(gpLocalCode, profileCell);
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
            }
        }

        //we don't retrieve from the DB and update so it will never be ID mapped
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
        /*boolean mapIds;
        if (practitionerBuilder.isIdMapped()) {
            mapIds = false;
        } else {
            mapIds = true;
        }
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(),mapIds, practitionerBuilder);*/
    }



    private static String getNationalIdTypeIdentifierSystem (String nationalIdType) {

        switch (nationalIdType.toUpperCase()) {
            case "GMC": return FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER;
            case "NMC": return FhirIdentifierUri.IDENTIFIER_SYSTEM_NMC_NUMBER;
            default: return null;
        }
    }

}
