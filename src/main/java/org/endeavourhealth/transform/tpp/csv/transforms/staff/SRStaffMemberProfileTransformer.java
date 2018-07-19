package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.StaffMemberProfileCache;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfile;
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
            while (parser.nextRecord()) {

                try {
                    createResource((SRStaffMemberProfile) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
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
        staffPojo.setIDStaffMember(staffId.getLong()); // Save id as Long for use as key.
        staffPojo.setIDStaffMemberCell(staffId);   // and as CsvCell for builders.

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
        if (!idProfileCreatedBy.isEmpty() && idProfileCreatedBy.getLong() > 0) {
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

        staffPojo.setParserState(parser.getCurrentState());

        //We have the pojo so write it out
        StaffMemberProfileCache.addStaffPojo(staffPojo);
        if ((StaffMemberProfileCache.size())%10000==0) { //Cache size every 10k records
            LOG.info("Staff member profile cache at " + StaffMemberProfileCache.size());
        }
    }
}
