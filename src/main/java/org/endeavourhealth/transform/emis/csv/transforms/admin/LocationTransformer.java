package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisAdminCacheFiler;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Location;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Reference;

import java.util.List;
import java.util.Map;

public class LocationTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        EmisAdminCacheFiler adminCacheFiler = new EmisAdminCacheFiler(csvHelper.getDataSharingAgreementGuid());

        AbstractCsvParser parser = parsers.get(Location.class);
        while (parser.nextRecord()) {

            try {
                createResource((Location)parser, fhirResourceFiler, csvHelper, adminCacheFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        adminCacheFiler.close();
    }

    private static void createResource(Location parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        LocationBuilder locationBuilder = new LocationBuilder();

        CsvCell locationGuid = parser.getLocationGuid();
        locationBuilder.setId(locationGuid.getString(), locationGuid);

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);

            //this resource exists in our admin resource cache, so we can populate the
            //main database when new practices come on, so we need to update that too
            adminCacheFiler.deleteAdminResourceFromCache(parser.getCurrentState(), locationBuilder.getResource());
            return;
        }

        CsvCell houseNameFlat = parser.getHouseNameFlatNumber();
        CsvCell numberAndStreet = parser.getNumberAndStreet();
        CsvCell village = parser.getVillage();
        CsvCell town = parser.getTown();
        CsvCell county = parser.getCounty();
        CsvCell postcode = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
        addressBuilder.setUse(Address.AddressUse.WORK);
        addressBuilder.addLine(houseNameFlat.getString(), houseNameFlat);
        addressBuilder.addLine(numberAndStreet.getString(), numberAndStreet);
        addressBuilder.addLine(village.getString(), village);
        addressBuilder.setTown(town.getString(), town);
        addressBuilder.setDistrict(county.getString(), county);
        addressBuilder.setPostcode(postcode.getString(), postcode);

        CsvCell phoneNumber = parser.getPhoneNumber();
        if (!phoneNumber.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.WORK, phoneNumber.getString());
            locationBuilder.addTelecom(fhirContact, phoneNumber);
        }

        CsvCell faxNumber = parser.getFaxNumber();
        if (!faxNumber.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.FAX, ContactPoint.ContactPointUse.WORK, faxNumber.getString());
            locationBuilder.addTelecom(fhirContact, faxNumber);
        }

        CsvCell email = parser.getEmailAddress();
        if (!email.isEmpty()) {
            ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.WORK, email.getString());
            locationBuilder.addTelecom(fhirContact, email);
        }

        CsvCell openDate = parser.getOpenDate();
        if (!openDate.isEmpty()) {
            locationBuilder.setOpenDate(openDate.getDate(), openDate);
        }

        CsvCell closeDate = parser.getCloseDate();
        if (!closeDate.isEmpty()) {
            locationBuilder.setCloseDate(closeDate.getDate(), closeDate);
        }

        CsvCell mainContactName = parser.getMainContactName();
        if (!mainContactName.isEmpty()) {
            locationBuilder.setMainContactName(mainContactName.getString(), mainContactName);
        }

        CsvCell name = parser.getLocationName();
        if (!name.isEmpty()) {
            locationBuilder.setName(name.getString(), name);
        }

        CsvCell type = parser.getLocationTypeDescription();
        if (!type.isEmpty()) {
            locationBuilder.setTypeFreeText(type.getString(), type);
        }

        CsvCell parentLocationGuid = parser.getParentLocationId();
        if (!parentLocationGuid.isEmpty()) {
            Reference locationReference = csvHelper.createLocationReference(parentLocationGuid);
            locationBuilder.setPartOf(locationReference, parentLocationGuid);
        }

        List<CsvCell> organisationCells = csvHelper.findOrganisationLocationMapping(locationGuid);
        if (organisationCells != null) {
            CsvCell organisationCell = organisationCells.get(0);
            Reference organisationReference = csvHelper.createOrganisationReference(organisationCell);
            locationBuilder.setManagingOrganisation(organisationReference, organisationCell);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), locationBuilder);
    }

    /*private static void createResource(Location parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       EmisAdminCacheFiler adminCacheFiler) throws Exception {

        org.hl7.fhir.instance.model.Location fhirLocation = new org.hl7.fhir.instance.model.Location();
        fhirLocation.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_LOCATION));

        String locationGuid = parser.getLocationGuid();
        fhirLocation.setId(locationGuid);

        if (parser.getDeleted()) {
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), fhirLocation);

            //this resource exists in our admin resource cache, so we can populate the
            //main database when new practices come on, so we need to update that too
            adminCacheFiler.deleteAdminResourceFromCache(parser.getCurrentState(), fhirLocation);
            return;
        }

        String houseNameFlat = parser.getHouseNameFlatNumber();
        String numberAndStreet = parser.getNumberAndStreet();
        String village = parser.getVillage();
        String town = parser.getTown();
        String county = parser.getCounty();
        String postcode = parser.getPostcode();

        Address fhirAddress = AddressConverter.createAddress(Address.AddressUse.WORK, houseNameFlat, numberAndStreet, village, town, county, postcode);
        fhirLocation.setAddress(fhirAddress);

        String phoneNumber = parser.getPhoneNumber();
        ContactPoint fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.PHONE, ContactPoint.ContactPointUse.WORK, phoneNumber);
        fhirLocation.addTelecom(fhirContact);

        String faxNumber = parser.getFaxNumber();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.FAX, ContactPoint.ContactPointUse.WORK, faxNumber);
        fhirLocation.addTelecom(fhirContact);

        String email = parser.getEmailAddress();
        fhirContact = ContactPointHelper.create(ContactPoint.ContactPointSystem.EMAIL, ContactPoint.ContactPointUse.WORK, email);
        fhirLocation.addTelecom(fhirContact);

        Date openDate = parser.getOpenDate();
        Date closeDate = parser.getCloseDate();
        boolean deleted = parser.getDeleted();
        Period fhirPeriod = PeriodHelper.createPeriod(openDate, closeDate);
        if (PeriodHelper.isActive(fhirPeriod) && !deleted) {
            fhirLocation.setStatus(org.hl7.fhir.instance.model.Location.LocationStatus.ACTIVE);
        } else {
            fhirLocation.setStatus(org.hl7.fhir.instance.model.Location.LocationStatus.INACTIVE);
        }
        fhirLocation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.ACTIVE_PERIOD, fhirPeriod));

        String mainContactName = parser.getMainContactName();
        if (!Strings.isNullOrEmpty(mainContactName)) {
            fhirLocation.addExtension(ExtensionConverter.createExtension(FhirExtensionUri.LOCATION_MAIN_CONTACT, new StringType(mainContactName)));
        }

        String name = parser.getLocationName();
        fhirLocation.setName(name);

        String type = parser.getLocationTypeDescription();
        fhirLocation.setType(CodeableConceptHelper.createCodeableConcept(type));

        String parentLocationGuid = parser.getParentLocationId();
        if (!Strings.isNullOrEmpty(parentLocationGuid)) {
            fhirLocation.setPartOf(csvHelper.createLocationReference(parentLocationGuid));
        }

        List<String> organisationGuids = csvHelper.findOrganisationLocationMapping(locationGuid);
        if (organisationGuids != null) {
            String organisationGuid = organisationGuids.get(0);
            fhirLocation.setManagingOrganization(csvHelper.createOrganisationReference(organisationGuid));
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), fhirLocation);

        //this resource exists in our admin resource cache, so we can populate the
        //main database when new practices come on, so we need to update that too
        adminCacheFiler.saveAdminResourceToCache(parser.getCurrentState(), fhirLocation);
    }*/
}
