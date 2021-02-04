package org.endeavourhealth.transform.homertonhi.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.Condition;
import org.endeavourhealth.transform.homertonhi.schema.ConditionDelete;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConditionTransformer  {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionTransformer.class);

    public static void transform(List<ParserI> parsers,
                                      FhirResourceFiler fhirResourceFiler,
                                      HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                        continue;
                    }
                    try {
                        transform((Condition) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void delete(List<ParserI> parsers,
                              FhirResourceFiler fhirResourceFiler,
                              HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        ConditionDelete conditionDeleteParser = (ConditionDelete) parser;
                        CsvCell hashValueCell = conditionDeleteParser.getHashValue();

                        //lookup the localId value set when the Condition was initially transformed
                        String conditionId = csvHelper.findLocalIdFromHashValue(hashValueCell);
                        if (!Strings.isNullOrEmpty(conditionId)) {
                            //get the resource to perform the deletion on
                            org.hl7.fhir.instance.model.Condition existingCondition
                                    = (org.hl7.fhir.instance.model.Condition) csvHelper.retrieveResourceForLocalId(ResourceType.Condition, conditionId);

                            if (existingCondition != null) {

                                ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
                                conditionBuilder.setDeletedAudit(hashValueCell);

                                //delete the condition resource. mapids is always false for deletions
                                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, conditionBuilder);
                            }
                        } else {
                            TransformWarnings.log(LOG, parser, "Delete failed. Unable to find Condition HASH_VALUE_TO_LOCAL_ID using hash_value: {}",
                                    hashValueCell.toString());
                        }
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(Condition parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell conditionIdCell = parser.getConditionId();

        ConditionBuilder conditionBuilder = new ConditionBuilder();
        conditionBuilder.setId(conditionIdCell.getString(), conditionIdCell);

        CsvCell personEmpiIdCell = parser.getPersonEmpiId();
        Reference patientReference
                = ReferenceHelper.createReference(ResourceType.Patient, personEmpiIdCell.getString());
        conditionBuilder.setPatient(patientReference, personEmpiIdCell);

        //NOTE:deletions are done using the hash values in the deletion transforms linking back to the local Id
        //so, save an InternalId link between the hash value and the local Id for this resource, i.e. condition_id
        CsvCell hashValueCell = parser.getHashValue();
        csvHelper.saveHashValueToLocalId(hashValueCell, conditionIdCell);

        CsvCell confirmationCell = parser.getConditionConfirmationStatusDisplay();
        if (!confirmationCell.isEmpty()) {

            String confirmation = confirmationCell.getString();
            if (confirmation.equalsIgnoreCase("confirmed")) {
                conditionBuilder.setVerificationStatus(org.hl7.fhir.instance.model.Condition.ConditionVerificationStatus.CONFIRMED, confirmationCell);

            } else {

                //only interested in Confirmed conditions/problems
                return;
            }
        } else {

            //only interested in Confirmed conditions/problems
            return;
        }

        //is it a problem or a diagnosis? Homerton send the type using a specific code
        CsvCell conditionTypeCodeCell = parser.getConditionTypeCode();
        if (conditionTypeCodeCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_CONDITION_PROBLEM)) {

            conditionBuilder.setAsProblem(true);
            conditionBuilder.setCategory("complaint", conditionTypeCodeCell);

            CsvCell problemStatusDisplayCell = parser.getProblemStatusDisplay();
            String problemStatus = problemStatusDisplayCell.getString();
            if (problemStatus.equalsIgnoreCase("active")) {

                conditionBuilder.setEndDateOrBoolean(null, problemStatusDisplayCell);

            } else if (problemStatus.equalsIgnoreCase("resolved")
                    || problemStatus.equalsIgnoreCase("inactive")
                    || problemStatus.equalsIgnoreCase("cancelled")) {

                //Status date confirmed as problem changed to Resolved/Inactive date for example
                CsvCell statusDateTimeCell = parser.getProblemStatusDtm();
                if (!statusDateTimeCell.isEmpty()) {

                    DateType dt = new DateType(statusDateTimeCell.getDateTime());
                    conditionBuilder.setEndDateOrBoolean(dt, problemStatusDisplayCell, statusDateTimeCell);

                } else {

                    //if we don't have a status date, use a boolean to indicate the end
                    conditionBuilder.setEndDateOrBoolean(new BooleanType(true), problemStatusDisplayCell);
                }
            }
        } else if (conditionTypeCodeCell.getString().equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_CONDITION_DIAGNOSIS)) {

            conditionBuilder.setAsProblem(false);
            conditionBuilder.setCategory("diagnosis", conditionTypeCodeCell);

            //an active Diagnosis
            conditionBuilder.setEndDateOrBoolean(null);

        } else {

            //catch any unknown condition types
            throw new TransformException("Unknown Condition type [" + conditionTypeCodeCell.getString() + "]");
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        if (!encounterIdCell.isEmpty()) {
            Reference encounterReference
                    = ReferenceHelper.createReference(ResourceType.Encounter, encounterIdCell.getString());
            conditionBuilder.setEncounter(encounterReference, encounterIdCell);
        }

        CsvCell effectiveDateTimeCell = parser.getEffectiveDtm();
        if (!effectiveDateTimeCell.isEmpty()) {

            DateTimeType dateTimeType = new DateTimeType(effectiveDateTimeCell.getDateTime());
            conditionBuilder.setOnset(dateTimeType, effectiveDateTimeCell);
        }

        // Conditions are coded either as Snomed or ICD10
        CsvCell conditionCodeCell = parser.getConditionRawCode();
        if (!conditionCodeCell.isEmpty()) {

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);

            CsvCell conditionCodeSystemCell = parser.getConditionCodingSystemId();
            if (!conditionCodeSystemCell.isEmpty()) {

                String conceptCodeSystem = conditionCodeSystemCell.getString();
                if (conceptCodeSystem.equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_SNOMED_URN)) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_SNOMED_CT, conditionCodeSystemCell);

                } else if (conceptCodeSystem.equalsIgnoreCase(HomertonHiCsvHelper.CODE_TYPE_ICD10_URN)) {

                    codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_ICD10, conditionCodeSystemCell);

                } else {

                    throw new TransformException("Unknown Condition code system [" + conceptCodeSystem + "]");
                }

                String conditionCode = conditionCodeCell.getString();
                codeableConceptBuilder.setCodingCode(conditionCode, conditionCodeCell);

                CsvCell conditionCodeDisplayCell = parser.getConditionDisplay();
                codeableConceptBuilder.setCodingDisplay(conditionCodeDisplayCell.getString(), conditionCodeDisplayCell);
                codeableConceptBuilder.setText(conditionCodeDisplayCell.getString(), conditionCodeDisplayCell);
            }
        } else {
            //if there's no code, create a non coded code so we retain the text from the non code element
            CsvCell termCell = parser.getConditionDescription();

            CodeableConceptBuilder codeableConceptBuilder
                    = new CodeableConceptBuilder(conditionBuilder, CodeableConceptBuilder.Tag.Condition_Main_Code);
            codeableConceptBuilder.setText(termCell.getString(), termCell);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), conditionBuilder);
    }
}