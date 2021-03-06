package org.endeavourhealth.transform.tpp.csv.schema.patient;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRPatientRelationship extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRPatientRelationship.class);

    public SRPatientRelationship(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(TppCsvToFhirTransformer.VERSION_88)
                || version.equals(TppCsvToFhirTransformer.VERSION_91)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "DateEnded",
                    "RelationshipType",
                    "PersonalGuardianOrProxy",
                    "NextOfKin",
                    "CaresForPatient",
                    "PrincipalCarerForPatient",
                    "KeyHolder",
                    "HasParentalResponsibility",
                    "FinancialRepresentative",
                    "CallCentreCallBackConsent",
                    "CopyCorrespondence",
                    "ContactOrder",
                    "ContactMethod",
                    "CommunicationFormat",
                    "InterpreterRequired",
                    "IDRelationshipWithPatient",
                    "IDPatientRelationshipWith",
                    "CodeRelationshipWithUser",
                    "RelationshipWithName",
                    "RelationshipWithDateOfBirth",
                    "RelationshipWithHouseName",
                    "RelationshipWithHouseNumber",
                    "RelationshipWithRoad",
                    "RelationshipWithLocality",
                    "RelationshipWithPostTown",
                    "RelationshipWithCounty",
                    "RelationshipWithPostCode",
                    "RelationshipWithTelephone",
                    "RelationshipWithWorkTelephone",
                    "RelationshipWithMobileTelephone",
                    "RelationshipWithFax",
                    "RelationshipWithEmailAddress",
                    "RelationshipWithSex",
                    "RelationshipWithSpokenLanguage",
                    "RelationshipWithOrganisation",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "DateEnded",
                    "RelationshipType",
                    "PersonalGuardianOrProxy",
                    "NextOfKin",
                    "CaresForPatient",
                    "PrincipalCarerForPatient",
                    "KeyHolder",
                    "HasParentalResponsibility",
                    "FinancialRepresentative",
                    "CallCentreCallBackConsent",
                    "CopyCorrespondence",
                    "ContactOrder",
                    "ContactMethod",
                    "CommunicationFormat",
                    "InterpreterRequired",
                    "IDRelationshipWithPatient",
                    "IDPatientRelationshipWith",
                    "CodeRelationshipWithUser",
                    "RelationshipWithName",
                    "RelationshipWithDateOfBirth",
                    "RelationshipWithHouseName",
                    "RelationshipWithHouseNumber",
                    "RelationshipWithRoad",
                    "RelationshipWithLocality",
                    "RelationshipWithPostTown",
                    "RelationshipWithCounty",
                    "RelationshipWithPostCode",
                    "RelationshipWithTelephone",
                    "RelationshipWithWorkTelephone",
                    "RelationshipWithMobileTelephone",
                    "RelationshipWithFax",
                    "RelationshipWithEmailAddress",
                    "RelationshipWithSex",
                    "RelationshipWithSpokenLanguage",
                    "RelationshipWithOrganisation",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };

        } else if (version.equals(TppCsvToFhirTransformer.VERSION_92)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "DateEnded",
                    "RelationshipType",
                    "PersonalGuardianOrProxy",
                    "NextOfKin",
                    "CaresForPatient",
                    "PrincipalCarerForPatient",
                    "KeyHolder",
                    "HasParentalResponsibility",
                    "FinancialRepresentative",
                    "Advocate",
                    "MainVisitor",
                    "CallCentreCallBackConsent",
                    "CopyCorrespondence",
                    "ContactOrder",
                    "ContactMethod",
                    "CommunicationFormat",
                    "InterpreterRequired",
                    "IDRelationshipWithPatient",
                    "IDPatientRelationshipWith",
                    "CodeRelationshipWithUser",
                    "RelationshipWithName",
                    "RelationshipWithDateOfBirth",
                    "RelationshipWithHouseName",
                    "RelationshipWithHouseNumber",
                    "RelationshipWithRoad",
                    "RelationshipWithLocality",
                    "RelationshipWithPostTown",
                    "RelationshipWithCounty",
                    "RelationshipWithPostCode",
                    "RelationshipWithTelephone",
                    "RelationshipWithWorkTelephone",
                    "RelationshipWithMobileTelephone",
                    "RelationshipWithFax",
                    "RelationshipWithEmailAddress",
                    "RelationshipWithSex",
                    "RelationshipWithSpokenLanguage",
                    "RelationshipWithOrganisation",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt"
            };

        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "DateEventRecorded",
                    "DateEvent",
                    "IDProfileEnteredBy",
                    "IDDoneBy",
                    "TextualEventDoneBy",
                    "IDOrganisationDoneAt",
                    "DateEnded",
                    "RelationshipType",
                    "PersonalGuardianOrProxy",
                    "NextOfKin",
                    "CaresForPatient",
                    "PrincipalCarerForPatient",
                    "KeyHolder",
                    "HasParentalResponsibility",
                    "FinancialRepresentative",
                    "Advocate",
                    "MainVisitor",
                    "CallCentreCallBackConsent",
                    "CopyCorrespondence",
                    "ContactOrder",
                    "ContactMethod",
                    "CommunicationFormat",
                    "InterpreterRequired",
                    "IDRelationshipWithPatient",
                    "IDPatientRelationshipWith",
                    "CodeRelationshipWithUser",
                    "RelationshipWithName",
                    "RelationshipWithDateOfBirth",
                    "RelationshipWithHouseName",
                    "RelationshipWithHouseNumber",
                    "RelationshipWithRoad",
                    "RelationshipWithLocality",
                    "RelationshipWithPostTown",
                    "RelationshipWithCounty",
                    "RelationshipWithPostCode",
                    "RelationshipWithTelephone",
                    "RelationshipWithWorkTelephone",
                    "RelationshipWithMobileTelephone",
                    "RelationshipWithFax",
                    "RelationshipWithEmailAddress",
                    "RelationshipWithSex",
                    "RelationshipWithSpokenLanguage",
                    "RelationshipWithOrganisation",
                    "IDEvent",
                    "IDPatient",
                    "IDOrganisation",
                    "IDOrganisationRegisteredAt",
                    "RemovedData"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateEventRecorded() {
        return super.getCell("DateEventRecorded");
    }

    public CsvCell getDateEvent() {
        return super.getCell("DateEvent");
    }

    public CsvCell getIDProfileEnteredBy() {
        return super.getCell("IDProfileEnteredBy");
    }

    public CsvCell getIDDoneBy() {
        return super.getCell("IDDoneBy");
    }

    public CsvCell getTextualEventDoneBy() {
        return super.getCell("TextualEventDoneBy");
    }

    public CsvCell getIDOrganisationDoneAt() {
        return super.getCell("IDOrganisationDoneAt");
    }

    public CsvCell getDateEnded() {
        return super.getCell("DateEnded");
    }

    public CsvCell getRelationshipType() {
        return super.getCell("RelationshipType");
    }

    public CsvCell getPersonalGuardianOrProxy() {
        return super.getCell("PersonalGuardianOrProxy");
    }

    public CsvCell getNextOfKin() {
        return super.getCell("NextOfKin");
    }

    public CsvCell getCaresForPatient() {
        return super.getCell("CaresForPatient");
    }

    public CsvCell getPrincipalCarerForPatient() {
        return super.getCell("PrincipalCarerForPatient");
    }

    public CsvCell getKeyHolder() {
        return super.getCell("KeyHolder");
    }

    public CsvCell getHasParentalResponsibility() {
        return super.getCell("HasParentalResponsibility");
    }

    public CsvCell getFinancialRepresentative() {
        return super.getCell("FinancialRepresentative");
    }

    public CsvCell getAdvocate() {
        return super.getCell("Advocate");
    }

    public CsvCell getMainVisitor() {
        return super.getCell("MainVisitor");
    }

    public CsvCell getCallCentreCallBackConsent() {
        return super.getCell("CallCentreCallBackConsent");
    }

    public CsvCell getCopyCorrespondence() {
        return super.getCell("CopyCorrespondence");
    }

    public CsvCell getContactOrder() {
        return super.getCell("ContactOrder");
    }

    public CsvCell getContactMethod() {
        return super.getCell("ContactMethod");
    }

    public CsvCell getCommunicationFormat() {
        return super.getCell("CommunicationFormat");
    }

    public CsvCell getInterpreterRequired() {
        return super.getCell("InterpreterRequired");
    }

    public CsvCell getIDRelationshipWithPatient() {
        return super.getCell("IDRelationshipWithPatient");
    }

    public CsvCell getIDPatientRelationshipWith() {
        return super.getCell("IDPatientRelationshipWith");
    }

    public CsvCell getCodeRelationshipWithUser() {
        return super.getCell("CodeRelationshipWithUser");
    }

    public CsvCell getRelationshipWithName() {
        return super.getCell("RelationshipWithName");
    }

    public CsvCell getRelationshipWithDateOfBirth() {
        return super.getCell("RelationshipWithDateOfBirth");
    }

    public CsvCell getRelationshipWithHouseName() {
        return super.getCell("RelationshipWithHouseName");
    }

    public CsvCell getRelationshipWithHouseNumber() {
        return super.getCell("RelationshipWithHouseNumber");
    }

    public CsvCell getRelationshipWithRoad() {
        return super.getCell("RelationshipWithRoad");
    }

    public CsvCell getRelationshipWithLocality() {
        return super.getCell("RelationshipWithLocality");
    }

    public CsvCell getRelationshipWithPostTown() {
        return super.getCell("RelationshipWithPostTown");
    }

    public CsvCell getRelationshipWithCounty() {
        return super.getCell("RelationshipWithCounty");
    }

    public CsvCell getRelationshipWithPostCode() {
        return super.getCell("RelationshipWithPostCode");
    }

    public CsvCell getRelationshipWithTelephone() {
        return super.getCell("RelationshipWithTelephone");
    }

    public CsvCell getRelationshipWithWorkTelephone() {
        return super.getCell("RelationshipWithWorkTelephone");
    }

    public CsvCell getRelationshipWithMobileTelephone() {
        return super.getCell("RelationshipWithMobileTelephone");
    }

    public CsvCell getRelationshipWithFax() {
        return super.getCell("RelationshipWithFax");
    }

    public CsvCell getRelationshipWithEmailAddress() {
        return super.getCell("RelationshipWithEmailAddress");
    }

    public CsvCell getRelationshipWithSex() {
        return super.getCell("RelationshipWithSex");
    }

    public CsvCell getRelationshipWithSpokenLanguage() {
        return super.getCell("RelationshipWithSpokenLanguage");
    }

    public CsvCell getRelationshipWithOrganisation() {
        return super.getCell("RelationshipWithOrganisation");
    }

    public CsvCell getIDEvent() {
        return super.getCell("IDEvent");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getIDOrganisationRegisteredAt() {
        return super.getCell("IDOrganisationRegisteredAt");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
