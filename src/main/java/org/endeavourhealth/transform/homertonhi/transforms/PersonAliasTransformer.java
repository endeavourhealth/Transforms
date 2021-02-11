package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonAlias;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonAliasTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonAliasTransformer.class);

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
                        transform((PersonAlias) parser, fhirResourceFiler, csvHelper);
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
                        PersonAlias personAliasParser = (PersonAlias) parser;
                        CsvCell hashValueCell = personAliasParser.getHashValue();

                        //lookup the localId value set when the PersonAlias was initially transformed
                        String personEmpiId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(personEmpiId)) {
                            //get the Patient resource to perform the Alias deletion from
                            Patient patient
                                    = (Patient) csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personEmpiId);

                            if (patient != null) {

                                PatientBuilder patientBuilder = new PatientBuilder(patient);

                                //if the identifer is found and removed, save the resource (with the identifier removed)
                                if (IdentifierBuilder.removeExistingIdentifierById(patientBuilder, hashValueCell.getString())) {

                                    //note, mapid = false as the resource is already saved / mapped
                                    fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, patientBuilder);
                                }
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Person Alias delete failed. Unable to find Person HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
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

    public static void transform(PersonAlias parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //if the alias is empty there's nothing to add
        CsvCell aliasCell = parser.getAlias();
        if (aliasCell.isEmpty()) {
            return;
        }

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. empi_id
        //i.e. the identifier belongs to a patient
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, personEmpiIdCell);

        //we always fully re-create the Identifier on the patient so just remove any previous instance
        //Use the has_value as the Id so it can be used for any single deletes in the _delete transform
        IdentifierBuilder identifierBuilder
                = IdentifierBuilder.findOrCreateForId(patientBuilder, hashValueCell);
        identifierBuilder.reset();

        //work out the system for the alias
        CsvCell aliasTypeDisplayCell = parser.getAliasTypeDisplay();
        String aliasSystem = convertAliasCode(aliasTypeDisplayCell.getString());

        if (aliasSystem == null) {
            TransformWarnings.log(LOG, parser, "Unknown alias system for {}", aliasTypeDisplayCell);
            aliasSystem = "UNKNOWN";
        }

        //if the alias record is an NHS number, then it's an official use. Secondary otherwise.
        Identifier.IdentifierUse use = null;
        if (aliasSystem.equalsIgnoreCase(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER)) {
            use = Identifier.IdentifierUse.OFFICIAL;

        } else {
            use = Identifier.IdentifierUse.SECONDARY;
        }

        identifierBuilder.setUse(use);
        identifierBuilder.setValue(aliasCell.getString(), aliasCell);
        identifierBuilder.setSystem(aliasSystem, aliasTypeDisplayCell);

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiIdCell, patientBuilder);
    }

    private static String convertAliasCode(String aliasCode) {
        switch (aliasCode.toUpperCase()) {
            case "MRN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_ROYAL_FREE_MRN_PATIENT_ID;
            case "NHS":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "NHS NUMBER":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER;
            case "CNN":
                return FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID;
            default:
                return null;
        }
    }
}