package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.reference.models.MultiLexToCTV3Map;
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
        while (parser.nextRecord()) {

            try {
                createResource((SRDrugSensitivity) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRDrugSensitivity parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      TppCsvHelper csvHelper) throws Exception {


        CsvCell rowId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

        AllergyIntoleranceBuilder allergyIntoleranceBuilder = new AllergyIntoleranceBuilder();
        TppCsvHelper.setUniqueId(allergyIntoleranceBuilder, patientId, rowId);

        allergyIntoleranceBuilder.setPatient(csvHelper.createPatientReference(patientId));

        CsvCell deleteData = parser.getRemovedData();
        if (deleteData.getIntAsBoolean()) {

            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), allergyIntoleranceBuilder);
            return;
        }

        CsvCell recordedBy = parser.getIDProfileEnteredBy();
        if (!recordedBy.isEmpty()) {

            String staffMemberId =
                    csvHelper.getInternalId (InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID, recordedBy.getString());
            Reference staffReference = csvHelper.createPractitionerReference(staffMemberId);
            allergyIntoleranceBuilder.setRecordedBy(staffReference, recordedBy);
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

            MultiLexToCTV3Map lookUpMultiLexToCTV3Map = csvHelper.lookUpMultiLexToCTV3Map(drugCodeId.getLong());
            if (lookUpMultiLexToCTV3Map != null) {
                // add Ctv3 coding
                CodeableConceptBuilder codeableConceptBuilder
                        = new CodeableConceptBuilder(allergyIntoleranceBuilder,  null);
                codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CTV3);
                codeableConceptBuilder.setCodingCode(lookUpMultiLexToCTV3Map.getCTV3ReadCode());
                codeableConceptBuilder.setCodingDisplay(lookUpMultiLexToCTV3Map.getCTV3ReadTerm());
                codeableConceptBuilder.setText(lookUpMultiLexToCTV3Map.getCTV3ReadTerm());
            }
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
