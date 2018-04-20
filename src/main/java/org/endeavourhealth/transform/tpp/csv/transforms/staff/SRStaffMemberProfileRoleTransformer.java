package org.endeavourhealth.transform.tpp.csv.transforms.staff;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.staff.SRStaffMemberProfileRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRStaffMemberProfileRoleTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRStaffMemberProfileRoleTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRStaffMemberProfileRole.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRStaffMemberProfileRole) parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRStaffMemberProfileRole parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        return;

    }
}
