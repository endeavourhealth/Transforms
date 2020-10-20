package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedParametersBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.HomertonRfCodeableConceptHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonDemographics;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PersonDemographicsTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PersonDemographicsTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {
                    try {
                        transform((PersonDemographics) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(PersonDemographics parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

        // Note: This transform adds any additional patient related demographics information to
        // the Patient resource which is not collected during the Person transformer such as
        // Marital status, Ethnicity, Religion and Cause of death if the patient is deceased

        CsvCell personEmpiCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //marital status
        CsvCell maritalStatusCode = parser.getMaritalStatusCernerCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(maritalStatusCode)) {

            CsvCell maritalMeaningCell
                    = HomertonRfCodeableConceptHelper.getCellMeaning(csvHelper, CodeValueSet.MARITAL_STATUS, maritalStatusCode);
            if (maritalMeaningCell == null) {
                TransformWarnings.log(LOG, parser, "ERROR: cerner marital status {} not found", maritalStatusCode);

            } else {
                MaritalStatus maritalStatus = convertMaritalStatus(maritalMeaningCell.getString(), parser);
                patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCode, maritalMeaningCell);
            }
        } else {
            //if updating a record, make sure to clear the field in this case
            patientBuilder.setMaritalStatus(null);
        }

        //ethnicity
        CsvCell ethnicityCode = parser.getEthnicityCernerCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(ethnicityCode)) {

            CsvCell ehtnicityCell
                    = HomertonRfCodeableConceptHelper.getCellAlias(csvHelper, CodeValueSet.ETHNIC_GROUP, ethnicityCode);
            if (ehtnicityCell == null) {
                TransformWarnings.log(LOG, parser, "ERROR: cerner ethnicity {} not found", ethnicityCode);

            } else {
                EthnicCategory ethnicCategory = convertEthnicCategory(ehtnicityCell.getString());
                patientBuilder.setEthnicity(ethnicCategory, ethnicityCode, ehtnicityCell);
            }

        } else {
            //if this field is empty we should clear the value from the patient
            patientBuilder.setEthnicity(null);
        }

        //religion
        CsvCell religionCell = parser.getReligionCernerCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(religionCell)) {
            CernerCodeValueRef cvref = csvHelper.lookupCodeRef(CodeValueSet.RELIGION, religionCell);

            //if possible, map the religion to the NHS data dictionary values
            Religion fhirReligion = mapReligion(cvref);
            if (fhirReligion != null) {
                patientBuilder.setReligion(fhirReligion, religionCell);
            } else {
                //if not possible to map, carry the value over as free text
                String freeTextReligion = cvref.getCodeDispTxt();
                patientBuilder.setReligionFreeText(freeTextReligion, religionCell);
            }

        } else {
            //it's an existing patient resource, so set to null as the religion field may have been cleared
            patientBuilder.setReligion(null);
        }


        //cause of death IM mappings if deceased
        CsvCell isDeceasedCell = parser.getIsDecesed();
        if (!isDeceasedCell.isEmpty() && isDeceasedCell.getBoolean() == true) {

            ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(patientBuilder);
            parametersBuilder.removeContainedParameters();

            CsvCell causeOfDeathCodeCell = parser.getCauseOfDeathCernerCode();

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Homerton","CM_Sys_Cerner","EDW","person_demographics",
                    "CAUSE_OF_DEATH_RAW_CODE");
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Homerton","CM_Sys_Cerner","EDW","person_demographics",
                    "CAUSE_OF_DEATH_RAW_CODE", causeOfDeathCodeCell.getString(), IMConstant.BARTS_CERNER );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiCell, patientBuilder);
    }

    private static MaritalStatus convertMaritalStatus(String statusCode, ParserI parser) throws Exception {
        switch (statusCode.toUpperCase()) {
            case "DIVORCED":
                return MaritalStatus.DIVORCED;
            case "MARRIED":
                return MaritalStatus.MARRIED;
            case "LGL_SPRTN":
                return MaritalStatus.LEGALLY_SEPARATED;
            case "SINGLE":
                return MaritalStatus.NEVER_MARRIED;
            case "UNKNOWN":
                return null;
            case "WIDOW":
                return MaritalStatus.WIDOWED;
            case "LIFE_PTNR":
                return MaritalStatus.DOMESTIC_PARTNER;
            default:
                TransformWarnings.log(LOG, parser, "Unmapped marital status {}", statusCode);
                return null;
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

    private static Religion mapReligion(CernerCodeValueRef cvref) throws Exception {
        String s = cvref.getAliasNhsCdAlias().toUpperCase();

        switch (s) {
            case "BAPTIST":
                return Religion.BAPTIST;
            case "BUDDHISM":
                return Religion.BUDDHIST;
            case "ROMANCATHOLIC":
                return Religion.ROMAN_CATHOLIC;
            case "HINDU":
                return Religion.HINDU;
            case "JEHOVAHSWITNESS":
                return Religion.JEHOVAHS_WITNESS;
            case "LUTHERAN":
                return Religion.LUTHERAN;
            case "METHODIST":
                return Religion.METHODIST;
            case "ISLAM":
                return Religion.MUSLIM;
            case "SEVENTHDAYADVENTIST":
                return Religion.SEVENTH_DAY_ADVENTIST;
            case "OTHER":
                return Religion.RELIGION_OTHER;
            case "JUDAISMJEWISHHEBREW":
                return Religion.JEWISH;
            case "PRESBYTERIAN":
                return Religion.PRESBYTERIAN;
            case "NAZARENE":
                return Religion.NAZARENE_CHURCH;
            case "LATTERDAYSAINTS":
                return Religion.MORMON;
            case "GREEKORTHODOX":
                return Religion.GREEK_ORTHODOX;
            case "PENTECOSTAL":
                return Religion.PENTECOSTALIST;
            case "CHURCHOFENGLAND":
                return Religion.CHURCH_OF_ENGLAND;
            case "ETHIOPIANORTHODOXRASTAFARIAN":
                return Religion.RASTAFARI;
            case "SIKH":
                return Religion.SIKH;
            case "PLYMOUTHBRETHREN":
                return Religion.PLYMOUTH_BRETHREN;
            case "CHRISTIANCHURCH":
                return Religion.CHRISTIAN;
            case "CHURCHOFGOD":
                return Religion.CHURCH_OF_GOD_OF_PROPHECY;
            case "ASSEMBLIESOFGOD":
                return null;
            case "CHURCHOFCHRIST":
                return null;
            case "EPISCOPAL":
                return null;
            default:
                throw new Exception("Unmapped religion " + s);
        }
    }
}
