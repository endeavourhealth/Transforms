package org.endeavourhealth.transform.barts.transforms.v2;

import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.core.database.dal.ehr.models.Patient;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.emis.openhr.transforms.common.SexConverter;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.Enumerations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class PPATITransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PPATITransformerV2.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser) parser)) {
                    continue;
                }

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                createPatient((PPATI) parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //this transform always UPDATES resources when possible, so we use the patient cache to retrieve from the DB
        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();

        //get new/cached/from db Patient
        Patient patient =  csvHelper.getPatientCache().borrowPatientV2Instance(millenniumPersonIdCell);
        if (patient == null) {
            return;
        }

        //if the PPATI record is marked as non-active, it means we should delete the patient.
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {

            //TODO:  delete patient record
            return;
        }

        try {
            CsvCell nhsNumberCell = parser.getNhsNumber();
            if (!nhsNumberCell.isEmpty()) {
                String nhsNumber = nhsNumberCell.getString();
                nhsNumber = nhsNumber.replace("-", ""); //Cerner NHS numbers are tokenised with hyphens, so remove

                patient.setNhsNumber(nhsNumber);
            }

            CsvCell dateOfBirthCell = parser.getDateOfBirth();
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(dateOfBirthCell)) {
                //we need to handle multiple formats, so attempt to apply both formats here
                Date dob = BartsCsvHelper.parseDate(dateOfBirthCell);

                patient.setDateOfBirth(dob);
            } else {
                //we may be updating an existing patient to null
                patient.setDateOfBirth(null);
            }

            CsvCell genderCell = parser.getGenderCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(genderCell)) {
                CsvCell genderMeaningCell = BartsCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.GENDER, genderCell);
                if (genderMeaningCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: Cerner gender code {} not found", genderCell);

                } else {
                    Enumerations.AdministrativeGender gender = SexConverter.convertCernerSexToFhir(genderMeaningCell.getString());
                    int genderConceptId
                            = IMHelper.getIMConcept(null, null, IMConstant.FHIR_ADMINISTRATIVE_GENDER, gender.toCode(), gender.getDisplay());
                    //TODO: set IM without  fhirResource?
                    patient.setGenderTypeId(genderConceptId);
                }
            } else {
                //if updating a record then clear the gender if the field is empty
                patient.setGenderTypeId(null);
            }

            CsvCell ethnicityCode = parser.getEthnicGroupCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(ethnicityCode)) {

                CsvCell ethnicityCell = BartsCodeableConceptHelper.getCellAlias(csvHelper, CodeValueSet.ETHNIC_GROUP, ethnicityCode);
                if (ethnicityCell == null) {
                    TransformWarnings.log(LOG, parser, "ERROR: Cerner ethnicity {} not found", ethnicityCode);

                } else {
                    EthnicCategory ethnicCategory = convertEthnicCategory(ethnicityCell.getString());
                    int ethnicCodeConceptId
                            = IMHelper.getIMConcept(null, null, IMConstant.FHIR_ETHNIC_CATEGORY, ethnicCategory.getCode(), ethnicCategory.getDescription());
                    //TODO: set IM map without fhirResource
                    patient.setEthnicCodeTypeId(ethnicCodeConceptId);
                }
            } else {
                //if this field is empty we should clear the value from the patient
                patient.setEthnicCodeTypeId(null);
            }

            // If we have a deceased date, set that but if not and the patient is deceased just set the deceased flag
            CsvCell deceasedDateTimeCell = parser.getDeceasedDateTime();
            if (!BartsCsvHelper.isEmptyOrIsEndOfTime(deceasedDateTimeCell)) {

                //could be in one of two format
                Date dod = BartsCsvHelper.parseDate(deceasedDateTimeCell);
                patient.setDateOfDeath(dod);

            } else {
                //if updating a record, we may have REMOVED a date of death set incorrectly, so clear the fields on the patient
                patient.setDateOfDeath(null);
            }

            //get the current address for the patient from the cache
            int corePatientId = patient.getId();
            Integer currentAddressId = csvHelper.findPatientCurrentAddressId(corePatientId);
            if (currentAddressId != null) {
                patient.setCurrentAddressId(currentAddressId);
            }

        } finally {
            //we don't save the patient here; there are subsequent transforms that work on the patients so we
            //save patients after all of them are done
            csvHelper.getPatientCache().returnPatientV2Instance(millenniumPersonIdCell, patient);
        }
    }

    private static EthnicCategory convertEthnicCategory(String aliasNhsCdAlias) {

        //the alias field on the Cerner code ref table matches the NHS Data Dictionary ethnicity values
        //except for 99, whcih means "not stated"
        if (aliasNhsCdAlias.equalsIgnoreCase("99")) {
            return EthnicCategory.NOT_STATED;

        } else {
            return EthnicCategory.fromCode(aliasNhsCdAlias);
        }
    }
}
