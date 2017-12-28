package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.transform.common.BasisTransformer;
import org.hl7.fhir.instance.model.Enumerations;

public class HomertonBasisTransformer extends BasisTransformer {
    //private static final Logger LOG = LoggerFactory.getLogger(HomertonBasisTransformer.class);


    public static Enumerations.AdministrativeGender convertGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.FEMALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.MALE;
            } else {
                return Enumerations.AdministrativeGender.UNKNOWN;
            }
        }
    }

}
