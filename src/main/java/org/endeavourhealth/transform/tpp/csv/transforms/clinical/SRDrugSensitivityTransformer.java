package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultiLexToCtv3Map;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AllergyIntoleranceBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRDrugSensitivity;
import org.hl7.fhir.instance.model.AllergyIntolerance;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class SRDrugSensitivityTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRDrugSensitivityTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRDrugSensitivity.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRDrugSensitivity) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(SRDrugSensitivity parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {


        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            AllergyIntolerance allergyIntolerance = (AllergyIntolerance)csvHelper.retrieveResource(rowId.getString(), ResourceType.AllergyIntolerance);
            if (allergyIntolerance != null) {
                AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder(allergyIntolerance);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, allergyIntoleranceBuilder);
            }
            return;
        }

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        allergyIntoleranceBuilder.setId(rowId.getString(), rowId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        allergyIntoleranceBuilder.setPatient(patientReference);

        CsvCell profileIdRecordedBy = parser.getIDProfileEnteredBy();
        if (!profileIdRecordedBy.isEmpty()) {
            Reference staffReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedBy);
            allergyIntoleranceBuilder.setRecordedBy(staffReference, profileIdRecordedBy);
        }

        CsvCell staffMemberIdDoneBy = parser.getIDDoneBy();
        if (!staffMemberIdDoneBy.isEmpty() && staffMemberIdDoneBy.getLong() > -1) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDProfileEnteredBy(), parser.getIDOrganisationDoneAt());
            allergyIntoleranceBuilder.setClinician(staffReference, staffMemberIdDoneBy);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {
            allergyIntoleranceBuilder.setRecordedDate(dateRecored.getDateTime(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateStarted();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate);
        }

        CsvCell endDate = parser.getDateEnded();
        if (!endDate.isEmpty()) {
            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.INACTIVE);
            allergyIntoleranceBuilder.setEndDate(endDate.getDate(), endDate);

        } else {
            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.ACTIVE);
        }

        // these are drug allergies
        allergyIntoleranceBuilder.setCategory(AllergyIntolerance.AllergyIntoleranceCategory.MEDICATION);

        // map multi-lex drug product code to CTV3 read for coded allergy substance
        CsvCell drugCodeId = parser.getIDDrugCode();
        if (!drugCodeId.isEmpty()) {

            TppMultiLexToCtv3Map lookUpMultiLexToCTV3Map = csvHelper.lookUpMultiLexToCTV3Map(drugCodeId.getLong(), parser);
            if (lookUpMultiLexToCTV3Map != null) {
                // add Ctv3 coding
                CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
                codeableConceptBuilder.setCodingCode(lookUpMultiLexToCTV3Map.getCtv3ReadCode());
                codeableConceptBuilder.setCodingDisplay(lookUpMultiLexToCTV3Map.getCtv3ReadTerm());
                codeableConceptBuilder.setText(lookUpMultiLexToCTV3Map.getCtv3ReadTerm());
            }
        }

        // get multi-lex action group / thereputic category
        CsvCell drugMultiLexActionId = parser.getIDMultiLexAction();
        if (!drugMultiLexActionId.isEmpty()) {
            //TODO: implement lookup
        }

        // set consultation/encounter reference
        CsvCell eventId = parser.getIDEvent();
        if (!eventId.isEmpty()) {
            Reference eventReference = csvHelper.createEncounterReference(eventId);
            allergyIntoleranceBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }
}
