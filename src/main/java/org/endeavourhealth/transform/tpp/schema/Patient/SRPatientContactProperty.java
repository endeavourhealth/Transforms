package org.endeavourhealth.transform.tpp.schema.Patient;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientContactProperty extends AbstractCsvParser {

 private static final Logger LOG = LoggerFactory.getLogger(SRPatientContactProperty.class); 

  public SRPatientContactProperty(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                      "IDPatient",
                      "PatientContactType",
                      "PatientContactId",
                      "PropertyType",
                      "Property"
                    

            };

        }
 public CsvCell getRowIdentifier() { return super.getCell("RowIdentifier");};
 public CsvCell getIDOrganisationVisibleTo() { return super.getCell("IDOrganisationVisibleTo");};
 public CsvCell getIDPatient() { return super.getCell("IDPatient");};
 public CsvCell getPatientContactType() { return super.getCell("PatientContactType");};
 public CsvCell getPatientContactId() { return super.getCell("PatientContactId");};
 public CsvCell getPropertyType() { return super.getCell("PropertyType");};
 public CsvCell getProperty() { return super.getCell("Property");};


 //TODO fix the string below to make it meaningful
     @Override
protected String getFileTypeDescription() {return "TPP SRPatientContactProperty Entry file ";}

     @Override
protected boolean isFileAudited() {return true;}
        }
