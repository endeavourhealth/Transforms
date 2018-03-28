package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PractitionerResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PractitionerResourceCache.class);

    private static Map<Long, PractitionerBuilder> practitionerBuildersById = new HashMap<>();

    public static PractitionerBuilder getPractitionerBuilder(CsvCell staffMemberIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        PractitionerBuilder practitionerBuilder = practitionerBuildersById.get(staffMemberIdCell.getLong());
        if (practitionerBuilder == null) {

            org.hl7.fhir.instance.model.Practitioner practitioner
                    = (org.hl7.fhir.instance.model.Practitioner)csvHelper.retrieveResource(staffMemberIdCell.getString(), ResourceType.Practitioner, fhirResourceFiler);
            if (practitioner == null) {
                //if the Practitioner doesn't exist yet, create a new one
                practitionerBuilder = new PractitionerBuilder();
                practitionerBuilder.setId(staffMemberIdCell.getString(), staffMemberIdCell);
            } else {
                practitionerBuilder = new PractitionerBuilder(practitioner);
            }

            practitionerBuildersById.put(staffMemberIdCell.getLong(), practitionerBuilder);
        }
        return practitionerBuilder;
    }

    public static void filePractitionerResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long staffMemberId: practitionerBuildersById.keySet()) {
            PractitionerBuilder practitionerBuilder = practitionerBuildersById.get(staffMemberId);
            fhirResourceFiler.saveAdminResource( null, practitionerBuilder);
        }

        //clear down as everything has been saved
        practitionerBuildersById.clear();
    }
}
