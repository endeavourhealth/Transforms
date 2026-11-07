package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultilexProductToCtv3Map;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
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

        CsvCell profileIdRecordedByCell = parser.getIDProfileEnteredBy();
        Reference recordedByReference = csvHelper.createPractitionerReferenceForProfileId(profileIdRecordedByCell);
        if (recordedByReference != null) {
            allergyIntoleranceBuilder.setRecordedBy(recordedByReference, profileIdRecordedByCell);
        }

        CsvCell staffMemberIdDoneByCell = parser.getIDDoneBy();
        CsvCell orgDoneAtCell = parser.getIDOrganisationDoneAt();
        Reference doneByReference = csvHelper.createPractitionerReferenceForStaffMemberId(staffMemberIdDoneByCell, orgDoneAtCell);
        if (doneByReference != null) {
            allergyIntoleranceBuilder.setClinician(doneByReference, staffMemberIdDoneByCell, orgDoneAtCell);
        }

        CsvCell dateRecorded = parser.getDateEventRecorded();
        if (!dateRecorded.isEmpty()) {
            allergyIntoleranceBuilder.setRecordedDate(dateRecorded.getDateTime(), dateRecorded);
        }

        CsvCell effectiveDate = parser.getDateStarted();
        if (!effectiveDate.isEmpty()) {
            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate);
        }

        CsvCell endDate = parser.getDateEnded();
        if (!endDate.isEmpty()) {
            //note, it's not possible to set a FUTURE end date on SystmOne drug sensitivities, so we don't
            //need to worry about ones with an end date set that are still currently active
            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.INACTIVE);
            allergyIntoleranceBuilder.setEndDate(endDate.getDate(), endDate);

        } else {
            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.ACTIVE);
        }

        // these are drug allergies
        allergyIntoleranceBuilder.setCategory(AllergyIntolerance.AllergyIntoleranceCategory.MEDICATION);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(allergyIntoleranceBuilder, CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code);

        //map Multilex product ID to CTV3 read for coded allergy substance
        CsvCell drugCodeIdCell = parser.getIDDrugCode();
        if (!TppCsvHelper.isEmptyOrNegative(drugCodeIdCell)) {

            TppMultilexProductToCtv3Map lookUpMultiLexToCTV3Map = csvHelper.lookUpMultilexToCTV3Map(drugCodeIdCell);
            if (lookUpMultiLexToCTV3Map != null) {

                //copy the CSV cell but with the CTV3 code so that the auditing from the original cell is carried over
                String ctv3Code = lookUpMultiLexToCTV3Map.getCtv3ReadCode();
                CsvCell ctv3CodeCell = CsvCell.factoryWithNewValue(drugCodeIdCell, ctv3Code);
                TppCodingHelper.addCodes(codeableConceptBuilder, null, null, ctv3CodeCell, null);

            } else {
                //if we fail to look up a CTV3 code for the Multilex product ID then we essentially know nothing about the
                //drug the patient is sensitive to, but only log this out if we also do not have the drug name (which is a new field)
                CsvCell drugNameCell = parser.getNameOfMedication();
                if (drugNameCell == null || drugNameCell.isEmpty()) {
                    TransformWarnings.log(LOG, csvHelper, "TPP Multilex lookup failed for product ID {}", drugCodeIdCell);
                }
            }
        }

        //get Multilex action group / thereputic category
        CsvCell drugMultilexActionIdCell = parser.getIDMultiLexAction();
        if (!TppCsvHelper.isEmptyOrNegative(drugMultilexActionIdCell)) {

            codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_TPP_DRUG_ACTION_GROUP);
            codeableConceptBuilder.setCodingCode(drugMultilexActionIdCell.getString(), drugMultilexActionIdCell);

            String name = csvHelper.lookUpMultilexActionGroupNameForId(drugMultilexActionIdCell);
            if (!Strings.isNullOrEmpty(name)) {

                codeableConceptBuilder.setCodingDisplay(name);
                codeableConceptBuilder.setText(name);
            }
        }

        //SD-453 - TPP have added a new column to give the actual drug name
        CsvCell drugNameCell = parser.getNameOfMedication();
        if (drugNameCell != null && !drugNameCell.isEmpty()) {
            codeableConceptBuilder.setText(drugNameCell.getString(), drugNameCell);
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
