package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.ContactPointHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisLocationDalI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Location;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class LocationTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Location parser = (Location)parsers.get(Location.class);
        if (parser != null) {

            while (parser.nextRecord()) {
                CsvCell locationGuid = parser.getLocationGuid();
                csvHelper.getAdminHelper().addLocationChanged(locationGuid);
            }

            //the above will have audited the table, so now we can load the bulk staging table with our file
            String filePath = parser.getFilePath();
            Date dataDate = fhirResourceFiler.getDataDate();
            EmisLocationDalI dal = DalProvider.factoryEmisLocationDal();
            int fileId = parser.getFileAuditId().intValue();
            dal.updateLocationStagingTable(filePath, dataDate, fileId);

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
        }
    }

    /*private static void createResource(Location parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        LocationBuilder locationBuilder = new LocationBuilder();

        CsvCell locationGuid = parser.getLocationGuid();
        locationBuilder.setId(locationGuid.getString(), locationGuid);

        CsvCell deleted = parser.getDeleted();
        if (deleted.getBoolean()) {
            locationBuilder.setDeletedAudit(deleted);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), locationBuilder);

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
        addressBuilder.setCity(town.getString(), town);
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
            CodeableConceptBuilder cc = new CodeableConceptBuilder(locationBuilder, CodeableConceptBuilder.Tag.Location_Type, true);
            cc.setText(type.getString(), type);
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
    }*/

}
