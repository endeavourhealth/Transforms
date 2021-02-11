package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonPhone;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonPhoneTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonPhoneTransformer.class);

    public static void transform(List<ParserI> parsers,
                                      FhirResourceFiler fhirResourceFiler,
                                      HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
                    try {
                        transform((PersonPhone) parser, fhirResourceFiler, csvHelper);
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
                        PersonPhone personPhoneParser = (PersonPhone) parser;
                        CsvCell hashValueCell = personPhoneParser.getHashValue();

                        //lookup the Patient localId value set when the PersonPhone was initially transformed
                        String personEmpiId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(personEmpiId)) {
                            //get the Patient resource to perform the Phone deletion from
                            Patient patient
                                    = (Patient) csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personEmpiId);

                            if (patient != null) {
                                PatientBuilder patientBuilder = new PatientBuilder(patient);

                                //if the contactpoint is found and removed, save the resource (with the contact point removed)
                                if (ContactPointBuilder.removeExistingContactPointById(patientBuilder, hashValueCell.getString())) {

                                    //note, mapids = false as the resource is already saved and mapped
                                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
                                }
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Person Phone delete failed. Unable to find Person HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
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

    public static void transform(PersonPhone parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. empi_id
        //i.e. the ContactPointBuilder belongs to a patient
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, personEmpiIdCell);

        //by removing from the patient and re-adding, we're constantly changing the order of the
        //contact points, so attempt to reuse the existing one for the ID
        ContactPointBuilder contactPointBuilder
                = ContactPointBuilder.findOrCreateForId(patientBuilder, hashValueCell);
        contactPointBuilder.reset();

        CsvCell phoneTypeDisplayCell = parser.getPhoneTypeDisplay();
        if (!phoneTypeDisplayCell.isEmpty()) {

            String phoneTypeDesc = phoneTypeDisplayCell.getString();
            ContactPoint.ContactPointUse use = convertPhoneType(phoneTypeDesc);
            contactPointBuilder.setUse(use, phoneTypeDisplayCell);
        }
        CsvCell phoneNumberCell = parser.getPhoneNumber();
        String phoneNumber = phoneNumberCell.getString();

        //just append the extension on to the number
        CsvCell extensionCell = parser.getPhoneExt();
        if (!extensionCell.isEmpty()) {
            phoneNumber += " " + extensionCell.getString();
        }
        contactPointBuilder.setValue(phoneNumber, phoneNumberCell, extensionCell);

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiIdCell, patientBuilder);
    }

    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType) throws Exception {

        //we're missing codes in the code ref table, so just handle by returning SOMETHING
        if (phoneType == null) {
            return null;
        }

        //this is based on the full list of types from CODE_REF where the set is 43
        switch (phoneType.toUpperCase()) {
            case "HOME":
            case "VHOME":
            case "PHOME":
            case "USUAL":
            case "PAGER PERS":
            case "FAX PERS":
            case "VERIFY":
                return ContactPoint.ContactPointUse.HOME;

            case "FAX BUS":
            case "PROFESSIONAL":
            case "SECBUSINESS":
            case "CARETEAM":
            case "PHONEEPRESCR":
            case "AAM": //Automated Answering Machine
            case "BILLING":
            case "PAGER ALT":
            case "PAGING":
            case "PAGER BILL":
            case "FAX BILL":
            case "FAX ALT":
            case "ALTERNATE":
            case "EXTSECEMAIL":
            case "INTSECEMAIL":
            case "FAXEPRESCR":
            case "EMC":  //Emergency Phone
            case "TECHNICAL":
            case "OS AFTERHOUR":
            case "OS PHONE":
            case "OS PAGER":
            case "OS BK OFFICE":
            case "OS FAX":
            case "BUSINESS":
                return ContactPoint.ContactPointUse.WORK;

            case "MOBILE":
                return ContactPoint.ContactPointUse.MOBILE;

            case "FAX PREV":
            case "PREVIOUS":
            case "PAGER PREV":
                return ContactPoint.ContactPointUse.OLD;

            case "FAX TEMP":
            case "TEMPORARY":
                return ContactPoint.ContactPointUse.TEMP;

            default:
                throw new TransformException("Unsupported phone type [" + phoneType + "]");
        }
    }
}
