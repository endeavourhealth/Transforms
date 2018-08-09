package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisAdminResourceCache;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.StaffMemberCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SRStaffMemberProfileTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfile.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
        if (parser != null) {
            EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(TppCsvHelper.ADMIN_CACHE_KEY);

            while (parser.nextRecord()) {

                try {
                    createResource((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper, adminCacheFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }

            adminCacheFiler.close();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    private static void createResource(SRStaffMemberProfile parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        //seems to be some bad data in this file, where we have a record that doesn't link to a staff member record
        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        CsvCell profileIdCell = parser.getRowIdentifier();
        CsvCell orgId = parser.getIDOrganisation();

        //both SRStaffMember and SRStaffMemberProfile feed into the same Practitioner resources,
        //and we can get one record updated without the other(s). So we need to re-retrieve our existing
        //Practitioner because we don't want to lose anything already on it.
        PractitionerBuilder practitionerBuilder = null;

        //rather than retrieve the ID mapped instance of the resource, retrieve the resource from the admin resource cache,
        //which gives us a non-ID mapped version to work with. This then lets us easily save our changes back to the admin resource cache
        EmisAdminResourceCache adminCacheResource = adminCacheFiler.getResourceFromCache(ResourceType.Practitioner, profileIdCell.getString());
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

        CsvCell removedDataCell = parser.getRemovedData();
        if (removedDataCell != null && removedDataCell.getIntAsBoolean()) { //null check required because the column isn't always present
            //if the profle has been removed, mark the practitioner as non-active but don't
            //delete, since data may reterence it
            practitionerBuilder.setActive(false, removedDataCell);

        } else {

            //since this may be an update to an existing profile, we need to make sure to remove any existing matching instance
            //from the practitioner resource
            String profileId = profileIdCell.getString();
            PractitionerRoleBuilder.removeRoleForId(practitionerBuilder, profileId);

            PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

            //set the profile ID on the role element, so we can match up when we get updates
            roleBuilder.setId(profileId, profileIdCell);

            // This is a candidate for refactoring with the same code in SRStaffMemberTransformer - maybe when I'm more certain of FhirResourceFiler
            if (!orgId.isEmpty()) {
                Reference organisationReference = csvHelper.createOrganisationReference(orgId.getString());
                roleBuilder.setRoleManagingOrganisation(organisationReference, orgId);
            }

            CsvCell roleStart = parser.getDateEmploymentStart();
            if (!roleStart.isEmpty()) {
                roleBuilder.setRoleStartDate(roleStart.getDate(), roleStart);
            }

            CsvCell roleEnd = parser.getDateEmploymentEnd();
            if (!roleEnd.isEmpty()) {
                roleBuilder.setRoleEndDate(roleEnd.getDate(), roleEnd);
            }

            CsvCell roleName = parser.getStaffRole();
            if (!roleName.isEmpty()) {
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, CodeableConceptBuilder.Tag.Practitioner_Role);
                codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
                codeableConceptBuilder.setCodingDisplay(roleName.getString(), roleName);
            }

            CsvCell ppaId = parser.getPPAID();
            if (!ppaId.isEmpty()) {
                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);

                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
                identifierBuilder.setValue(ppaId.getString(), ppaId);
            }

            CsvCell gpLocalCode = parser.getGPLocalCode();
            if (!gpLocalCode.isEmpty()) {
                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);

                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
                identifierBuilder.setValue(gpLocalCode.getString(), gpLocalCode);
            }

            CsvCell gmpCode = parser.getGmpID();
            if (!gmpCode.isEmpty()) {
                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);

                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
                identifierBuilder.setValue(gmpCode.getString(), gmpCode);
            }
        }

        StaffMemberCacheObj cachedStaff = csvHelper.getStaffMemberCache().getStaffMemberObj(staffId, profileIdCell);
        if (cachedStaff != null) {
            StaffMemberCache.addOrUpdatePractitionerDetails(practitionerBuilder, cachedStaff, csvHelper);
        }

        adminCacheFiler.saveAdminResourceToCache(practitionerBuilder);

        //if we're not interested in this practitioner, then don't save to the main DB
        if (csvHelper.getStaffMemberCache().shouldSavePractitioner(practitionerBuilder, csvHelper)) {
            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), practitionerBuilder);
        }
    }
}
