package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Enumerations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class HomertonBasisTransformer extends BasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(HomertonBasisTransformer.class);

    public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, CsvCell personIdCell, UUID personUUID, HomertonCsvHelper csvHelper, ParserI parser) throws Exception {
        return null;
    }

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

    public static ResourceId getPrimaryOrgResourceId(ParserI parser, String primaryOrgOdsCode, FhirResourceFiler fhirResourceFiler) throws Exception {
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "Homerton University Hospital NHS Foundation Trust", "Homerton Row", "London", "", "", "E9 6SR");
        return resolveOrganisationResource(parser.getCurrentState(),primaryOrgOdsCode,fhirResourceFiler, "Homerton University Hospital NHS Foundation Trust",fhirOrgAddress);
    }


}
