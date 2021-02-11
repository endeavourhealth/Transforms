package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Person;
import org.endeavourhealth.transform.homertonhi.schema.PersonDelete;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(PersonTransformer.class);

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
                        transform((Person) parser, fhirResourceFiler, csvHelper);
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
                        PersonDelete personDeleteParser = (PersonDelete) parser;
                        CsvCell hashValueCell = personDeleteParser.getHashValue();

                        //lookup the localId value set when the Person was initially transformed
                        String personEmpiId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(personEmpiId)) {
                            //get the resource to perform the deletion on
                            Patient patient
                                    = (Patient) csvHelper.retrieveResourceForLocalId(ResourceType.Patient, personEmpiId);

                            if (patient != null) {

                                PatientBuilder patientBuilder = new PatientBuilder(patient);
                                patientBuilder.setDeletedAudit(hashValueCell);

                                //delete the patient resource. mapids is always false for deletions
                                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, patientBuilder);
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Person delete failed. Unable to find Person HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
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

    public static void transform(Person parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonHiCsvHelper csvHelper) throws Exception {

        // first up, get or create the Homerton Hospital / Royal Free Hospital organisations from the data source
        // Every extract has a source_description type field.  The Person file has once for each of the main
        // data items and one is always present for date of birth, so use that.  If it is not present, throw an
        // exception for now.  The remaining clinical transforms will use these cached builders.

        OrganizationBuilder organizationBuilder;
        CsvCell birthDateSourceDescriptionCell = parser.getBirthDateSourceDescription();
        if (!birthDateSourceDescriptionCell.isEmpty()) {

            String sourceOrgDescription = birthDateSourceDescriptionCell.getString().trim();
            organizationBuilder
                    = csvHelper.getOrganisationCache().getOrCreateOrganizationBuilder(sourceOrgDescription, csvHelper, fhirResourceFiler, parser);
            if (organizationBuilder == null) {
                TransformWarnings.log(LOG, parser, "Error creating Organization resource for source description: {}",
                        sourceOrgDescription);
                return;
            }
        } else {

            throw new Exception("birth_date_source_description column is empty for Person record: "+parser.getPersonEmpiId());
        }

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. empi_id
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, personEmpiIdCell);

        patientBuilder.setActive(true);

        String sourceOrgDescription = birthDateSourceDescriptionCell.getString().trim();
        String orgOds = csvHelper.getOrganisationCache().getOdsFromOrgName(sourceOrgDescription);
        Reference organisationReference = csvHelper.createOrganisationReference(orgOds);
        if (patientBuilder.isIdMapped()) {

            organisationReference
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
        }
        patientBuilder.setManagingOrganisation(organisationReference, birthDateSourceDescriptionCell);
        csvHelper.getOrganisationCache().returnOrganizationBuilder(sourceOrgDescription, organizationBuilder);

        //remove existing name if set - this is the first place names are set and are added to using PersonName transformers
        NameBuilder.removeExistingNameById(patientBuilder, personEmpiIdCell.getString());
        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(personEmpiIdCell.getString(), personEmpiIdCell);  //so it can be removed if exists

        CsvCell nameTypeDisplayCell = parser.getPersonNameTypeDisplay();
        if (!nameTypeDisplayCell.isEmpty()) {

            HumanName.NameUse nameUse
                    = convertNameUse(nameTypeDisplayCell.getString(), true);  //this name is their active name
            nameBuilder.setUse(nameUse, nameTypeDisplayCell);
        }

        CsvCell familyNameCell = parser.getPersonNameFamily();
        CsvCell givenName1Cell = parser.getPersonNameGiven1();
        CsvCell givenName2Cell = parser.getPersonNameGiven2();
        CsvCell titleCell = parser.getPersonNameTitle();
        CsvCell prefixCell = parser.getPersonNamePrefix();
        CsvCell suffixCell = parser.getPersonNameSuffix();

        if (!titleCell.isEmpty()) {

            nameBuilder.addPrefix(titleCell.getString(), titleCell);
        }
        if (!prefixCell.isEmpty()) {

            nameBuilder.addPrefix(prefixCell.getString(), prefixCell);
        }
        if (!givenName1Cell.isEmpty()) {

            nameBuilder.addGiven(givenName1Cell.getString(), givenName1Cell);
        }
        if (!givenName2Cell.isEmpty()) {

            nameBuilder.addGiven(givenName2Cell.getString(), givenName2Cell);
        }
        if (!familyNameCell.isEmpty()) {

            nameBuilder.addFamily(familyNameCell.getString(), familyNameCell);
        }
        if (!suffixCell.isEmpty()) {

            nameBuilder.addSuffix(suffixCell.getString(), suffixCell);
        }

        //NOTE: phone numbers supported by person_phone transform
        //CsvCell phoneNumberCell = parser.getPhoneNumber();

        // Gender
        CsvCell genderCell = parser.getGenderCode();
        if (!genderCell.isEmpty()) {

            Enumerations.AdministrativeGender fhirGender = convertGenderToFHIR(genderCell.getString());
            patientBuilder.setGender(fhirGender, genderCell);
        }

        // Date of birth (date only)
        CsvCell dobCell = parser.getBirthDate();
        if (!dobCell.isEmpty()) {

            patientBuilder.setDateOfBirth(dobCell.getDate(), dobCell);
        }

        // Deceased date if present (date and time)
        CsvCell dodCell = parser.getDeceasedDtTm();
        if (!dodCell.isEmpty()) {

            patientBuilder.setDateOfDeath(dodCell.getDateTime(), dodCell);
        }

        // NOTES: address data handled by person_address transform

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiIdCell, patientBuilder);
    }

    public static Enumerations.AdministrativeGender convertGenderToFHIR(String gender) {
        if (gender.equalsIgnoreCase("female")) {
            return Enumerations.AdministrativeGender.FEMALE;
        } else {
            if (gender.equalsIgnoreCase("male")) {
                return Enumerations.AdministrativeGender.MALE;
            } else {
                if (gender.equalsIgnoreCase("other")) {
                    return Enumerations.AdministrativeGender.OTHER;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
        }
    }

    private static HumanName.NameUse convertNameUse(String statusCode, boolean isActive) {

        //FHIR spec states that any ended name should be flagged as OLD
        if (!isActive) {
            return HumanName.NameUse.OLD;
        }

        switch (statusCode.toUpperCase()) {
            case "ADOPTED":
                return HumanName.NameUse.OFFICIAL;
            case "ALTERNATE":
                return HumanName.NameUse.NICKNAME;
            case "CURRENT":
                return HumanName.NameUse.OFFICIAL;
            case "LEGAL":
                return HumanName.NameUse.OFFICIAL;
            case "MAIDEN":
                return HumanName.NameUse.MAIDEN;
            case "OTHER":
                return HumanName.NameUse.TEMP;
            case "PREFERRED":
                return HumanName.NameUse.USUAL;
            case "PREVIOUS":
                return HumanName.NameUse.OLD;
            case "PRSNL":
                return HumanName.NameUse.TEMP;
            case "NYSIIS":
                return HumanName.NameUse.TEMP;
            case "ALT_CHAR_CUR":
                return HumanName.NameUse.NICKNAME;
            case "USUAL":
                return HumanName.NameUse.USUAL;
            case "HEALTHCARD":
                return HumanName.NameUse.TEMP;
            case "BACHELOR":
                return HumanName.NameUse.OLD;
            case "BIRTH":
                return HumanName.NameUse.OLD;
            case "NONHIST":
                return HumanName.NameUse.OLD;
            default:
                return null;
        }
    }
}