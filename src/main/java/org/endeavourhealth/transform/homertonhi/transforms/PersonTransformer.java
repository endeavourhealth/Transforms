package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.HomertonHiCodeableConceptHelper;
import org.endeavourhealth.transform.homertonhi.schema.Person;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class PersonTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {
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

    public static void transform(Person parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonHiCsvHelper csvHelper) throws Exception {

        // first up, get or create the Homerton organisation from the service
        UUID serviceId = parser.getServiceId();
        OrganizationBuilder organizationBuilder
                = csvHelper.getOrganisationCache().getOrCreateOrganizationBuilder(serviceId, csvHelper, fhirResourceFiler, parser);
        if (organizationBuilder == null) {
            TransformWarnings.log(LOG, parser, "Error creating Organization resource for ServiceId: {}",
                    serviceId.toString());
            return;
        }

        CsvCell personEmpiCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //NOTE:deletions are checked by comparing the deletion hash values set up in the deletion pre-transform
        CsvCell hashValueCell = parser.getHashValue();
        boolean deleted = false;  //TODO: requires pre-transform per file to establish deletions
        if (deleted) {
            patientBuilder.setDeletedAudit(hashValueCell);
            csvHelper.getPatientCache().deletePatient(patientBuilder, personEmpiCell, fhirResourceFiler, parser.getCurrentState());
            return;
        }
        patientBuilder.setActive(true);

        Reference organisationReference = csvHelper.createOrganisationReference(serviceId.toString());
        if (patientBuilder.isIdMapped()) {
            organisationReference
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(organisationReference, fhirResourceFiler);
        }
        patientBuilder.setManagingOrganisation(organisationReference);
        csvHelper.getOrganisationCache().returnOrganizationBuilder(serviceId, organizationBuilder);

        //remove existing name if set - this is the first place names are set and are added to using PersonName transformers
        NameBuilder.removeExistingNameById(patientBuilder, personEmpiCell.getString());

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setId(personEmpiCell.getString(), personEmpiCell);  //so it can be removed if exists

        CsvCell nameTypeCernerCodeCell = parser.getPersonNameTypeCernerCode();
        CsvCell codeMeaningCell
                = HomertonHiCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.NAME_USE, nameTypeCernerCodeCell);
        HumanName.NameUse nameUse
                = convertNameUse(codeMeaningCell.getString(), true);  //this name is their active name
        nameBuilder.setUse(nameUse, nameTypeCernerCodeCell, codeMeaningCell);

        CsvCell familyNameCell = parser.getPersonNameFamily();
        CsvCell givenName1Cell = parser.getPersonNameGiven1();
        CsvCell givenName2Cell = parser.getPersonNameGiven2();
        CsvCell titleCell = parser.getPersonNameTitle();
        CsvCell prefixCell = parser.getPersonNamePrefix();
        CsvCell suffixCell = parser.getPersonNameSuffix();

        nameBuilder.addPrefix(titleCell.getString(), titleCell);
        nameBuilder.addPrefix(prefixCell.getString(), prefixCell);
        nameBuilder.addGiven(givenName1Cell.getString(), givenName1Cell);
        nameBuilder.addGiven(givenName2Cell.getString(), givenName2Cell);
        nameBuilder.addFamily(familyNameCell.getString(), familyNameCell);
        nameBuilder.addSuffix(suffixCell.getString(), suffixCell);

        CsvCell phoneNumberCell = parser.getPhoneNumber();
        if (!phoneNumberCell.isEmpty()) {

            //use the personEmpid as the identifier for the main phone number
            ContactPointBuilder contactPointBuilder
                    = ContactPointBuilder.findOrCreateForId(patientBuilder, personEmpiCell);
            contactPointBuilder.reset();

            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);   //this is always a phone
            CsvCell getPhoneTypeCodeCell = parser.getPhoneTypeCode();
            CsvCell phoneTypeDescCell
                    = HomertonHiCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.PHONE_TYPE, getPhoneTypeCodeCell);
            ContactPoint.ContactPointUse use
                    = convertPhoneType(phoneTypeDescCell.getString(), true);  //their active phone type
            contactPointBuilder.setUse(use, getPhoneTypeCodeCell, phoneTypeDescCell);

            String phoneNumber = phoneNumberCell.getString();
            CsvCell phoneExtCell = parser.getPhoneExt();
            if (!phoneExtCell.isEmpty()) {
                phoneNumber += " " + phoneExtCell.getString();
            }
            contactPointBuilder.setValue(phoneNumber, phoneNumberCell, phoneExtCell);
        }

        // Gender
        CsvCell genderCell = parser.getGenderCode();
        if (!genderCell.isEmpty()) {

            Enumerations.AdministrativeGender fhirGender = convertGenderToFHIR(genderCell.getString());
            patientBuilder.setGender(fhirGender, genderCell);
        }

        // Date of birth
        CsvCell dobCell = parser.getBirthDate();
        if (!dobCell.isEmpty()) {
            patientBuilder.setDateOfBirth(dobCell.getDateTime(), dobCell);
        }

        // Deceased date if present
        CsvCell dodCell = parser.getDeceasedDtTm();
        if (!dodCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dodCell.getDateTime(), dobCell);
        }

        // remove existing address if set
        AddressBuilder.removeExistingAddressById(patientBuilder, personEmpiCell.getString());

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);

        CsvCell typeCell = parser.getAddressTypeCernerCode();
        CsvCell typeDescCell
                = HomertonHiCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
        String typeDesc = typeDescCell.getString();

        Address.AddressUse use = convertAddressUse(typeDesc, true);   //their active address
        if (use != null) {
            addressBuilder.setUse(use, typeCell, typeDescCell);
        }
        addressBuilder.setId(personEmpiCell.getString(), personEmpiCell);  //so it can be removed

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
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiCell, patientBuilder);
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

    private static ContactPoint.ContactPointUse convertPhoneType(String phoneType, boolean isActive) throws Exception {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return ContactPoint.ContactPointUse.OLD;
        }

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
