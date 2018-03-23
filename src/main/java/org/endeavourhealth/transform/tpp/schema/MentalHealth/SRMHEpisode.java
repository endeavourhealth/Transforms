package org.endeavourhealth.transform.tpp.schema.MentalHealth;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMHEpisode extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRMHEpisode.class); 

  public SRMHEpisode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "MHEpisodeStartDate",
                      "MHEpisodeEndDate",
                      "MHEpisodeStaff",
                      "CareCoOrdinator",
                      "NamedNurse",
                      "LeadProfessional",
                      "DateCreation",
                      "IDProfileCreatedBy",
                      "IDTeam",
                      "IDReferralIn",
                      "IDEvent",
                      "IDPatient",
                      "IDOrganisation",
                      "RemovedData"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getMHEpisodeStartDate() { return super.getCell("MHEpisodeStartDate");};
 public CsvCell getMHEpisodeEndDate() { return super.getCell("MHEpisodeEndDate");};
 public CsvCell getMHEpisodeStaff() { return super.getCell("MHEpisodeStaff");};
 public CsvCell getCareCoOrdinator() { return super.getCell("CareCoOrdinator");};
 public CsvCell getNamedNurse() { return super.getCell("NamedNurse");};
 public CsvCell getLeadProfessional() { return super.getCell("LeadProfessional");};
 public CsvCell getDateCreation() { return super.getCell("DateCreation");};
 public CsvCell getIDProfileCreatedBy() { return super.getCell("IDProfileCreatedBy");};
 public CsvCell getIDTeam() { return super.getCell("IDTeam");};
 public CsvCell getIDReferralIn() { return super.getCell("IDReferralIn");};
 public CsvCell getIDEvent() { return super.getCell("IDEvent");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};
 public CsvCell getRemovedData() { return super.getCell("RemovedData");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRMHEpisode Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
