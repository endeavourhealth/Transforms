package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisLocationDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Location;
import org.endeavourhealth.transform.emis.csv.schema.admin.OrganisationLocation;

import java.util.Date;
import java.util.Map;

public class OrganisationLocationTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        OrganisationLocation parser = (OrganisationLocation)parsers.get(OrganisationLocation.class);
        if (parser != null) {

            while (parser.nextRecord()) {
                //just iterate to make sure all records are audited
            }

            //the above will have audited the table, so now we can load the bulk staging table with our file
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            EmisLocationDalI dal = DalProvider.factoryEmisLocationDal();
            int fileId = parser.getFileAuditId().intValue();
            dal.updateOrganisationLocationStagingTable(filePath, dataDate, fileId);

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        }
    }


    /*private static void createLocationOrganisationMapping(OrganisationLocation parser,
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
    }*/
}
