package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultilexProductToCtv3Map;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AllergyIntoleranceBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCodingHelper;
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
                allergyIntoleranceBuilder.setDeletedAudit(deleteData);
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
        if (!TppCsvHelper.isEmptyOrNegative(staffMemberIdDoneBy)) {
            Reference staffReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneBy, parser.getIDOrganisationDoneAt());
            if (staffReference != null) {
                allergyIntoleranceBuilder.setClinician(staffReference, staffMemberIdDoneBy);
            }
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

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);

        // map multi-lex drug product code to CTV3 read for coded allergy substance
        CsvCell drugCodeIdCell = parser.getIDDrugCode();
        if (!TppCsvHelper.isEmptyOrNegative(drugCodeIdCell)) {

            TppMultilexProductToCtv3Map lookUpMultiLexToCTV3Map = csvHelper.lookUpMultilexToCTV3Map(drugCodeIdCell);
            if (lookUpMultiLexToCTV3Map != null) {

                //copy the CSV cell but with the CTV3 code so that the auditing from the original cell is carried over
                String ctv3Code = lookUpMultiLexToCTV3Map.getCtv3ReadCode();
                CsvCell ctv3CodeCell = CsvCell.factoryWithNewValue(drugCodeIdCell, ctv3Code);
                TppCodingHelper.addCodes(codeableConceptBuilder, null, null, ctv3CodeCell, null);
            }
        }

        // get multi-lex action group / thereputic category
        CsvCell drugMultilexActionIdCell = parser.getIDMultiLexAction();
        if (!TppCsvHelper.isEmptyOrNegative(drugMultilexActionIdCell)) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_TPP_DRUG_ACTION_GROUP);
            codeableConceptBuilder.setCodingCode(drugMultilexActionIdCell.getString());

            String name = csvHelper.lookUpMultilexActionGroupNameForId(drugMultilexActionIdCell);
            if (!Strings.isNullOrEmpty(name)) {

                codeableConceptBuilder.setCodingDisplay(name);
                codeableConceptBuilder.setText(name);
            }
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
