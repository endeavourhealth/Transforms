package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.LOREF;
import org.endeavourhealth.transform.barts.schema.ORGREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class ORGREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREFTransformer.class);
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {
                try {
                    createLocation((ORGREF) parser, (FhirResourceFiler) fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    /*
     *
     */
    public static void createLocation(ORGREF parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BartsCsvHelper csvHelper,
                                      String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        OrganizationBuilder organizationBuilder = new OrganizationBuilder();
        if (parser.getOrdId().isEmpty()) {
            return;
        }
        CsvCell orgIdCell = parser.getOrdId();
        organizationBuilder.setId(orgIdCell.getString());
        if (!parser.getOrgNameText().isEmpty()) {
            organizationBuilder.setName(parser.getOrgNameText().getString());
        }
        if (!parser.getNhsOrgAlias().isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER);
            identifierBuilder.setId(parser.getNhsOrgAlias().getString());
        }

        if (!parser.getParentNhsOrgAlias().isEmpty()) {
            Reference reference = ReferenceHelper.createReference(ResourceType.Organization, parser.getParentNhsOrgAlias().getString());
            organizationBuilder.setParentOrganisation(reference);
        }

        AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
        if (!parser.getAddrLine1Txt().isEmpty()) {
            addressBuilder.addLine(parser.getAddrLine1Txt().getString());
        }
        if (!parser.getAddrLine2Txt().isEmpty()) {
            addressBuilder.addLine(parser.getAddrLine2Txt().getString());
        }
        if (!parser.getAddrLine3Txt().isEmpty()) {
            addressBuilder.addLine(parser.getAddrLine3Txt().getString());
        }
        if (!parser.getAddrLine4Txt().isEmpty()) {
            addressBuilder.addLine(parser.getAddrLine4Txt().getString());
        }
        if (!parser.getPostCodeTxt().isEmpty()) {
            addressBuilder.setPostcode(parser.getPostCodeTxt().getString());
        }
        if (!parser.getCityTxt().isEmpty()) {
            addressBuilder.setTown(parser.getCityTxt().getString());
        }
        if (!parser.getPhoneNumberTxt().isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE, parser.getPhoneNumberTxt());
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
        }
        if (!parser.getFaxNbrTxt().isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.FAX, parser.getFaxNbrTxt());
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
        }

        if (!parser.getEmailTxt().isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(organizationBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL, parser.getEmailTxt());
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
        }

        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), organizationBuilder);
    }


}
