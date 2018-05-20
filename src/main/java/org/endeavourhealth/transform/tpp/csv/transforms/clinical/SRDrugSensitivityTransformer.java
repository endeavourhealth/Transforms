package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMultiLexToCtv3Map;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AllergyIntoleranceBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRDrugSensitivity;
import org.hl7.fhir.instance.model.AllergyIntolerance;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
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
    }

    public static void createResource(SRDrugSensitivity parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {


        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if ((deleteData != null) && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.AllergyIntolerance allergyIntolerance
                        = (org.hl7.fhir.instance.model.AllergyIntolerance) csvHelper.retrieveResource(rowId.getString(),
                        ResourceType.AllergyIntolerance,
                        fhirResourceFiler);

                if (allergyIntolerance != null) {
                    AllergyIntoleranceBuilder allergyIntoleranceBuilder
                            = new AllergyIntoleranceBuilder(allergyIntolerance);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
                }
                return;

            }
        }

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        allergyIntoleranceBuilder.setId(rowId.getString(), rowId);

        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
                allergyIntoleranceBuilder.setRecordedBy(staffReference, recordedBy);
            }
        }

        CsvCell procedureDoneBy = parser.getIDDoneBy();
        if (!procedureDoneBy.isEmpty()) {

            Reference staffReference = csvHelper.createPractitionerReference(procedureDoneBy);
            allergyIntoleranceBuilder.setClinician(staffReference, procedureDoneBy);
        }

        CsvCell dateRecored = parser.getDateEventRecorded();
        if (!dateRecored.isEmpty()) {

            allergyIntoleranceBuilder.setRecordedDate(dateRecored.getDate(), dateRecored);
        }

        CsvCell effectiveDate = parser.getDateStarted();
        if (!effectiveDate.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDate.getDate());
            allergyIntoleranceBuilder.setOnsetDate(dateTimeType, effectiveDate);
        }

        CsvCell endDate = parser.getDateEnded();
        if (!endDate.isEmpty()) {

            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.INACTIVE);
            DateTimeType dateTimeType = new DateTimeType(endDate.getDate());
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            String displayDate = sdf.format(dateTimeType);
            allergyIntoleranceBuilder.setNote("Date ended: "+displayDate);

        } else {

            allergyIntoleranceBuilder.setStatus(AllergyIntolerance.AllergyIntoleranceStatus.ACTIVE);
        }

        // these are drug allergies
        allergyIntoleranceBuilder.setCategory(AllergyIntolerance.AllergyIntoleranceCategory.MEDICATION);

        // map multi-lex drug product code to CTV3 read for coded allergy substance
        CsvCell drugCodeId = parser.getIDDrugCode();
        if (!drugCodeId.isEmpty()) {

            TppMultiLexToCtv3Map lookUpMultiLexToCTV3Map = csvHelper.lookUpMultiLexToCTV3Map(drugCodeId.getLong());
            if (lookUpMultiLexToCTV3Map != null) {
                // add Ctv3 coding
                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(allergyIntoleranceBuilder,  null);
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

            Reference eventReference = csvHelper.createEncounterReference(eventId, patientId);
            allergyIntoleranceBuilder.setEncounter (eventReference, eventId);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
    }
}
