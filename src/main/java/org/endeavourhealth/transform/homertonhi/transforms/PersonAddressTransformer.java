package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCodeableConceptHelper;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonAddress;
import org.endeavourhealth.transform.homertonhi.schema.PersonAddressDelete;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonAddressTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PersonAddressTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                        continue;
                    }
                    try {
                        transform((PersonAddress) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void delete(List<ParserI> parsers,
                              FhirResourceFiler fhirResourceFiler,
                              HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        PersonAddressDelete personAddressDeleteParser = (PersonAddressDelete) parser;
                        CsvCell hashValueCell = personAddressDeleteParser.getHashValue();

                        //lookup the localId value set when the Person was initially transformed
                        String personEmpiId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(personEmpiId)) {
                            //get the resource to perform the deletion on
                            Patient patient
                                    = (Patient) csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personEmpiId);

                            if (patient != null) {

                                PatientBuilder patientBuilder = new PatientBuilder(patient);

                                //if the Address is found and removed, save the resource (with the address removed)
                                if (AddressBuilder.removeExistingAddressById(patientBuilder, hashValueCell.getString())) {

                                    //note, mapids = false as the resource is already saved / mapped
                                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
                                }
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Person Address delete failed. Unable to find Person HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
                                    hashValueCell.toString());
                        }
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(PersonAddress parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonHiCsvHelper csvHelper) throws Exception {


        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. empi_id
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, personEmpiIdCell);

        // remove existing address if set using the hash_value as the unique id
        AddressBuilder.removeExistingAddressById(patientBuilder, hashValueCell.getString());

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);

        CsvCell typeCell = parser.getAddressTypeCernerCode();
        CsvCell typeDescCell
                = HomertonHiCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
        String typeDesc = typeDescCell.getString();

        Address.AddressUse use = convertAddressUse(typeDesc, true);   //their active address
        if (use != null) {
            addressBuilder.setUse(use, typeCell, typeDescCell);
        }
        addressBuilder.setId(hashValueCell.getString(), hashValueCell);  //so it can be removed / deleted

        CsvCell line1Cell = parser.getAddressLine1();
        CsvCell line2Cell = parser.getAddressLine2();
        CsvCell line3Cell = parser.getAddressLine3();
        CsvCell cityCell = parser.getAddressCity();
        CsvCell countyCell = parser.getAddressCounty();
        CsvCell postcodeCell = parser.getAddressPostCode();

        addressBuilder.addLine(line1Cell.getString(), line1Cell);
        addressBuilder.addLine(line2Cell.getString(), line2Cell);
        addressBuilder.addLine(line3Cell.getString(), line3Cell);
        addressBuilder.setCity(cityCell.getString(), cityCell);
        addressBuilder.setDistrict(countyCell.getString(), countyCell);
        addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiIdCell, patientBuilder);
    }

    private static Address.AddressUse convertAddressUse(String typeDesc, boolean isActive) throws TransformException {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return Address.AddressUse.OLD;
        }

        //NOTE there are 20+ address types in CVREF, but only the types known to be used are mapped below
        if (typeDesc.equalsIgnoreCase("Birth Address")
                || typeDesc.equalsIgnoreCase("home")
                || typeDesc.equalsIgnoreCase("mailing")) {
            return Address.AddressUse.HOME;

        } else if (typeDesc.equalsIgnoreCase("business")) {
            return Address.AddressUse.WORK;

        } else if (typeDesc.equalsIgnoreCase("temporary")
                || typeDesc.equalsIgnoreCase("Alternate Address")) {
            return Address.AddressUse.TEMP;

        } else if (typeDesc.equalsIgnoreCase("Prevous Address")) { //note the wrong spelling is in the Cerner data CVREF file
            return Address.AddressUse.OLD;

        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressType(..) too
            throw new TransformException("Unhandled type [" + typeDesc + "]");
        }
    }
}