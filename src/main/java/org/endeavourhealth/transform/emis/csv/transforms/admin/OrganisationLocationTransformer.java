package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.OrganisationLocation;

import java.util.Map;

public class OrganisationLocationTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        AbstractCsvParser parser = parsers.get(OrganisationLocation.class);
        while (parser != null && parser.nextRecord()) {

            try {
                createLocationOrganisationMapping((OrganisationLocation)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }


    private static void createLocationOrganisationMapping(OrganisationLocation parser,
                                                          FhirResourceFiler fhirResourceFiler,
                                                          EmisCsvHelper csvHelper) throws Exception {

        //if an org-location link has been deleted, then either a) the location has been deleted
        //in which case we'll sort it out because we'll have the deleted row in the Location CSV or
        //b) it's now part of a new organisation, in which case we'll have a new non-deleted row
        //in this CSV. In both cases, it's safe to simply ignore the deleted records in this file.
        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            return;
        }

        CsvCell orgGuid = parser.getOrgansationGuid();
        CsvCell locationGuid = parser.getLocationGuid();
        CsvCell mainLocation = parser.getIsMainLocation();

        boolean isMainLocation;
        if (parser.getVersion().equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            //NOTE in the emis test pack, the IsMainLocation column in the OrganisationLocation file is not in the
            //standard true/false format and is 1/0 instead, hence this special handler for that version
            isMainLocation = mainLocation.getInt() > 0;
        } else {
            isMainLocation = mainLocation.getBoolean();
        }

        csvHelper.cacheOrganisationLocationMap(locationGuid, orgGuid, isMainLocation);
    }
}
