package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PractitionerResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class SRStaffMemberProfileTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRStaffMemberProfile)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRStaffMemberProfile parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        // get practitioner builder from the cache
        CsvCell staffMemberId = parser.getIDStaffMember();
        PractitionerBuilder practitionerBuilder
                = PractitionerResourceCache.getPractitionerBuilder(staffMemberId, csvHelper, fhirResourceFiler);

        PractitionerRoleBuilder roleBuilder = new PractitionerRoleBuilder(practitionerBuilder);

        CsvCell orgID = parser.getIDOrganisation();
        Reference organisationReference = csvHelper.createOrganisationReference(orgID.getString());
        roleBuilder.setRoleManagingOrganisation(organisationReference, orgID);

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
            CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(roleBuilder, PractitionerRoleBuilder.TAG_ROLE_CODEABLE_CONCEPT);
            codeableConceptBuilder.addCoding(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
            codeableConceptBuilder.setCodingDisplay(roleName.getString(),roleName);
        }

        CsvCell ppaId = parser.getPPAID();
        if (!ppaId.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_DOCTOR_INDEX_NUMBER);
            identifierBuilder.setValue(ppaId.getString(), ppaId);
        }

        CsvCell gpLocalCode = parser.getGPLocalCode();
        if (!gpLocalCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_TPP_STAFF_GP_LOCAL_CODE);
            identifierBuilder.setValue(gpLocalCode.getString(), gpLocalCode);
        }

        CsvCell gmpCode = parser.getGmpID();
        if (!gmpCode.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
            identifierBuilder.setValue(gmpCode.getString(), gmpCode);
        }

    }
}
