package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ORGREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContactPointBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ORGREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREFTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                try {
                    createLocation((ORGREF)parser, (FhirResourceFiler)fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createLocation(ORGREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        OrganizationBuilder organizationBuilder = null;

        //we share Organization resources with the ADT feed, so make sure to retrieve an existing one to update
        CsvCell orgIdCell = parser.getOrgId();
        Organization existingOrg = (Organization)csvHelper.retrieveResourceForLocalId(ResourceType.Organization, orgIdCell);
        if (existingOrg == null) {
            organizationBuilder = new OrganizationBuilder();
            organizationBuilder.setId(orgIdCell.getString(), orgIdCell);

        } else {
            organizationBuilder = new OrganizationBuilder(existingOrg);
        }


        CsvCell orgNameCell = parser.getOrgNameText();
        if (!orgNameCell.isEmpty()) {
            organizationBuilder.setName(orgNameCell.getString(), orgNameCell);
        }

        CsvCell orgAliasCell = parser.getNhsOrgAlias(); //ODS code
        if (!orgAliasCell.isEmpty()) {
            IdentifierBuilder.removeExistingIdentifiersForSystem(organizationBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(orgAliasCell.getString(), orgAliasCell);
        }

        CsvCell parentOrgAliasCell = parser.getParentNhsOrgAlias();
        if (!parentOrgAliasCell.isEmpty()) {
            //confusingly, the file links orgs to its parents using the ODS code, not the ID, so we need
            //to look up the ID for our parent using the ODS code
            String parentOdsCode = parentOrgAliasCell.getString();

            //there are some records that have themselves as their parent, so
            //check for this and ignore that
            String odsCode = orgAliasCell.getString();
            if (odsCode == null
                || !parentOdsCode.equals(odsCode)) {

                String parentId = csvHelper.getInternalId(InternalIdMap.TYPE_CERNER_ODS_CODE_TO_ORG_ID, parentOdsCode);
                if (!Strings.isNullOrEmpty(parentId)) {
                    Reference reference = ReferenceHelper.createReference(ResourceType.Organization, parentId);
                    organizationBuilder.setParentOrganisation(reference);
                }
            }
        }

        //need to remove any existing address
        AddressBuilder.removeExistingAddresses(organizationBuilder);

        CsvCell addressLine1Cell = parser.getAddrLine1Txt();
        CsvCell addressLine2Cell = parser.getAddrLine2Txt();
        CsvCell addressLine3Cell = parser.getAddrLine3Txt();
        CsvCell addressLine4Cell = parser.getAddrLine4Txt();
        CsvCell postcodeCell = parser.getPostCodeTxt();
        CsvCell cityCell = parser.getCityTxt();

        if (!addressLine1Cell.isEmpty()
                || !addressLine2Cell.isEmpty()
                || !addressLine3Cell.isEmpty()
                || !addressLine4Cell.isEmpty()
                || !cityCell.isEmpty()
                || !postcodeCell.isEmpty()) {

            AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
            addressBuilder.setUse(Address.AddressUse.WORK);
            addressBuilder.addLine(addressLine1Cell.getString(), addressLine1Cell);
            addressBuilder.addLine(addressLine2Cell.getString(), addressLine2Cell);
            addressBuilder.addLine(addressLine3Cell.getString(), addressLine3Cell);
            addressBuilder.addLine(addressLine4Cell.getString(), addressLine4Cell);
            addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);
            addressBuilder.setCity(cityCell.getString(), cityCell);
        }

        //remove any existing phone numbers before we add new ones
        ContactPointBuilder.removeExistingContactPoints(organizationBuilder);

        CsvCell phoneNumberCell = parser.getPhoneNumberTxt();
        if (!phoneNumberCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(phoneNumberCell.getString(), phoneNumberCell);
        }

        CsvCell faxCell = parser.getFaxNbrTxt();
        if (!faxCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.FAX);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(faxCell.getString(), faxCell);
        }

        CsvCell emailCell = parser.getEmailTxt();
        if (!emailCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(emailCell.getString(), emailCell);
        }

        boolean mapIds = !organizationBuilder.isIdMapped();
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), mapIds, organizationBuilder);
    }


}
