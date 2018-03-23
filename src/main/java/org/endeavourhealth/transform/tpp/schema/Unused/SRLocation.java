package org.endeavourhealth.transform.tpp.schema.Unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRLocation extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRLocation.class); 

  public SRLocation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
            super(serviceId, systemId, exchangeId, version, filePath,
                    TppCsvToFhirTransformer.CSV_FORMAT,
                    TppCsvToFhirTransformer.DATE_FORMAT,
                    TppCsvToFhirTransformer.TIME_FORMAT);
        }


        @Override
        protected String[] getCsvHeaders(String version) {
            return new String[]{
                      "RowIdentifier",
                      "IDOrganisationVisibleTo",
                      "LocationName",
                      "LocationShortName",
                      "IDLocationParent",
                      "LocationUse",
                      "ADTSiteCode",
                      "CDSSiteCode",
                      "LocationNotes",
                      "WardOpeningHours",
                      "WardCareIntensity",
                      "WardIntendedAgeGroup",
                      "WardBroadPatientGroup",
                      "DateCreated",
                      "DateDeleted",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getLocationName() { return super.getCell("LocationName");};
 public CsvCell getLocationShortName() { return super.getCell("LocationShortName");};
 public CsvCell getIDLocationParent() { return super.getCell("IDLocationParent");};
 public CsvCell getLocationUse() { return super.getCell("LocationUse");};
 public CsvCell getADTSiteCode() { return super.getCell("ADTSiteCode");};
 public CsvCell getCDSSiteCode() { return super.getCell("CDSSiteCode");};
 public CsvCell getLocationNotes() { return super.getCell("LocationNotes");};
 public CsvCell getWardOpeningHours() { return super.getCell("WardOpeningHours");};
 public CsvCell getWardCareIntensity() { return super.getCell("WardCareIntensity");};
 public CsvCell getWardIntendedAgeGroup() { return super.getCell("WardIntendedAgeGroup");};
 public CsvCell getWardBroadPatientGroup() { return super.getCell("WardBroadPatientGroup");};
 public CsvCell getDateCreated() { return super.getCell("DateCreated");};
 public CsvCell getDateDeleted() { return super.getCell("DateDeleted");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRLocation Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
