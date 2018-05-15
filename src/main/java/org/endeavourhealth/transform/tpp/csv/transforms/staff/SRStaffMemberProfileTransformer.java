package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerRoleBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.PractitionerResourceCache;
import org.endeavourhealth.transform.tpp.cache.StaffMemberProfileCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
import org.hl7.fhir.instance.model.Reference;

import java.util.Map;

public class SRStaffMemberProfileTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMemberProfile.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void createResource(SRStaffMemberProfile parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        //

        CsvCell staffId = parser.getIDStaffMember(); //NB = rowId in SRStaffMember
        if (staffId.isEmpty()) {
            return;
        }

        StaffMemberProfilePojo staffPojo = new StaffMemberProfilePojo();
        staffPojo.setIDStaffMember(staffId.getLong());

        CsvCell staffMemberProfileId = parser.getRowIdentifier();
        staffPojo.setRowIdentifier(staffMemberProfileId);

        CsvCell orgId = parser.getIDOrganisation();
        if (!orgId.isEmpty()) { //shouldn't really happen, but there are a small number, so leave them without an org reference
            staffPojo.setIDOrganisation(orgId);
        }

        CsvCell roleStart = parser.getDateEmploymentStart();
        if (!roleStart.isEmpty()) {
            staffPojo.setDateEmploymentStart(roleStart);
        }

        CsvCell roleEnd = parser.getDateEmploymentEnd();
        if (!roleEnd.isEmpty()) {
            staffPojo.setDateEmploymentEnd(roleEnd);
        }

        CsvCell roleName = parser.getStaffRole();
        if (!roleName.isEmpty()) {
            staffPojo.setStaffRole(roleName);
        }

        CsvCell ppaId = parser.getPPAID();
        if (!ppaId.isEmpty()) {
            staffPojo.setPPAID(ppaId);
        }

        CsvCell gpLocalCode = parser.getGPLocalCode();
        if (!gpLocalCode.isEmpty()) {
            staffPojo.setGPLocalCode(gpLocalCode);
        }

        CsvCell gmpCode = parser.getGmpID();
        if (!gmpCode.isEmpty()) {
            staffPojo.setGmpID(gmpCode);
        }

        CsvCell idProfileCreatedBy = parser.getIdProfileCreatedBy();
        if (!idProfileCreatedBy.isEmpty()) {
            staffPojo.setIdProfileCreatedBy(idProfileCreatedBy);
        }

        CsvCell dateProfileCreated = parser.getDateProfileCreated();
        if (!dateProfileCreated.isEmpty()) {
            staffPojo.setDateProfileCreated(dateProfileCreated);
        }

        CsvCell IDStaffMemberProfileRole = parser.getIDStaffMemberProfileRole();
        if (!IDStaffMemberProfileRole.isEmpty()) {
            staffPojo.setIDStaffMemberProfileRole(IDStaffMemberProfileRole);
        }


        //We have the pojo so write it out
        StaffMemberProfileCache.addStaffPojo(staffPojo);
    }
}
