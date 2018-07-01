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
import org.hl7.fhir.instance.model.ContactPoint;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
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
    }


    public static void createLocation(ORGREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();

        CsvCell orgIdCell = parser.getOrgId();
        organizationBuilder.setId(orgIdCell.getString(), orgIdCell);

        CsvCell orgNameCell = parser.getOrgNameText();
        if (!orgNameCell.isEmpty()) {
            organizationBuilder.setName(orgNameCell.getString(), orgNameCell);
        }

        CsvCell orgAliasCell = parser.getNhsOrgAlias();
        if (!orgAliasCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_ODS_CODE);
            identifierBuilder.setValue(orgAliasCell.getString(), orgAliasCell);
            //identifierBuilder.setId(orgAliasCell.getString(), orgAliasCell);
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

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);

        CsvCell addressLine1Cell = parser.getAddrLine1Txt();
        if (!addressLine1Cell.isEmpty()) {
            addressBuilder.addLine(addressLine1Cell.getString(), addressLine1Cell);
        }

        CsvCell addressLine2Cell = parser.getAddrLine2Txt();
        if (!addressLine2Cell.isEmpty()) {
            addressBuilder.addLine(addressLine2Cell.getString(), addressLine2Cell);
        }

        CsvCell addressLine3Cell = parser.getAddrLine3Txt();
        if (!addressLine3Cell.isEmpty()) {
            addressBuilder.addLine(addressLine3Cell.getString(), addressLine3Cell);
        }

        CsvCell addressLine4Cell = parser.getAddrLine4Txt();
        if (!addressLine4Cell.isEmpty()) {
            addressBuilder.addLine(addressLine4Cell.getString(), addressLine4Cell);
        }

        CsvCell postcodeCell = parser.getPostCodeTxt();
        if (!postcodeCell.isEmpty()) {
            addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);
        }

        CsvCell cityCell = parser.getCityTxt();
        if (!cityCell.isEmpty()) {
            addressBuilder.setTown(cityCell.getString(), cityCell);
        }

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

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }


}
