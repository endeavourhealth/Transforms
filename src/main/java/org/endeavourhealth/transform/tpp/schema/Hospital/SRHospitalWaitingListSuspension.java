package org.endeavourhealth.transform.tpp.schema.Hospital;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRHospitalWaitingListSuspension extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRHospitalWaitingListSuspension.class); 

  public SRHospitalWaitingListSuspension(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "DateStart",
                      "DateEnd",
                      "SuspensionReason",
                      "WaitingListName",
                      "SuspensionType",
                      "Notes",
                      "DateClinicalReview",
                      "IDHospitalWaitingList",
                      "IDPatient",
                      "IDOrganisation"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getDateStart() { return super.getCell("DateStart");};
 public CsvCell getDateEnd() { return super.getCell("DateEnd");};
 public CsvCell getSuspensionReason() { return super.getCell("SuspensionReason");};
 public CsvCell getWaitingListName() { return super.getCell("WaitingListName");};
 public CsvCell getSuspensionType() { return super.getCell("SuspensionType");};
 public CsvCell getNotes() { return super.getCell("Notes");};
 public CsvCell getDateClinicalReview() { return super.getCell("DateClinicalReview");};
 public CsvCell getIDHospitalWaitingList() { return super.getCell("IDHospitalWaitingList");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getIDOrganisation() { return super.getCell("IDOrganisation");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRHospitalWaitingListSuspension Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
