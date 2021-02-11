package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.PersonDemographics;
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

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }
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
        // NOTE: patient_demographics_delete not implemented as it is specific to the entire patient,
        // which is handled via the parson_delete instead, i.e. no individual demographics referenced and deleted

        CsvCell personEmpiCell = parser.getPersonEmpiId();
        PatientBuilder patientBuilder = csvHelper.getPatientCache().getPatientBuilder(personEmpiCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }

        //marital status
        CsvCell maritalStatusCodeCell = parser.getMaritalStatusCode();
        if (!maritalStatusCodeCell.isEmpty()) {

            MaritalStatus maritalStatus = convertMaritalStatus(maritalStatusCodeCell.getString(), parser);
            patientBuilder.setMaritalStatus(maritalStatus, maritalStatusCodeCell);
        } else {
            //if updating a record, make sure to clear the field in this case
            patientBuilder.setMaritalStatus(null);
        }

        //ethnicity
        CsvCell ethnicityCodeCell = parser.getEthnicityCode();
        if (!ethnicityCodeCell.isEmpty()) {

            EthnicCategory ethnicCategory = convertEthnicCategory(ethnicityCodeCell.getString(), parser);
            patientBuilder.setEthnicity(ethnicCategory, ethnicityCodeCell);
        } else {
            //if this field is empty we should clear the value from the patient
            patientBuilder.setEthnicity(null);
        }

        //religion
        CsvCell religionDisplayCell = parser.getReligionDisplay();
        if (!religionDisplayCell.isEmpty()) {

            //if possible, map the religion to the NHS data dictionary values
            Religion fhirReligion = mapReligion(religionDisplayCell.getString());
            if (fhirReligion != null) {

                patientBuilder.setReligion(fhirReligion, religionDisplayCell);
            } else {
                //if not possible to map, carry the value over as free text
                patientBuilder.setReligionFreeText(religionDisplayCell.getString(), religionDisplayCell);
            }

        } else {
            //it's an existing patient resource, so set to null as the religion field may have been cleared
            patientBuilder.setReligion(null);
        }

        //date of death - non causes of death data detected in live
        CsvCell isDeceasedCell = parser.getIsDeceased();
        if (!isDeceasedCell.isEmpty() && isDeceasedCell.getBoolean()) {

            // Deceased date if present
            CsvCell dodCell = parser.getDeceasedDtm();
            if (!dodCell.isEmpty()) {
                patientBuilder.setDateOfDeath(dodCell.getDateTime(), dodCell);
            }
        }

        //no need to save the resource now, as all patient resources are saved at the end of the Patient transform section
        //here we simply return the patient builder to the cache
        csvHelper.getPatientCache().returnPatientBuilder(personEmpiCell, patientBuilder);
    }

    private static MaritalStatus convertMaritalStatus(String statusCode, ParserI parser) throws Exception {
        switch (statusCode.toUpperCase()) {
            case "D":
                return MaritalStatus.DIVORCED;
            case "M":
                return MaritalStatus.MARRIED;
            case "L":
                return MaritalStatus.LEGALLY_SEPARATED;
            case "S":
                return MaritalStatus.NEVER_MARRIED;
            case "W":
                return MaritalStatus.WIDOWED;
            default:
                TransformWarnings.log(LOG, parser, "Unmapped marital status {}", statusCode);
                return null;
        }
    }

    private static EthnicCategory convertEthnicCategory(String ethnicCode, ParserI parser) throws Exception {

        EthnicCategory category = null;
        try {

            category = EthnicCategory.fromCode(ethnicCode);
        } catch (IllegalArgumentException ex) {

            //handle non standard ethnic data and return null
            TransformWarnings.log(LOG, parser, "Unmapped Ethnic code {}", ethnicCode);
            return null;
        }
        return category;
    }

    private static Religion mapReligion(String religionText) throws Exception {

        //remove spaces and make uppercase for crude text matching
        String s = religionText.replaceAll(" ","").toUpperCase();

        switch (s) {
            case "BAPTIST":
                return Religion.BAPTIST;
            case "BUDDHISM":
                return Religion.BUDDHIST;
            case "BUDDHIST":
                return Religion.BUDDHIST;
            case "ROMANCATHOLIC":
                return Religion.ROMAN_CATHOLIC;
            case "HINDU":
                return Religion.HINDU;
            case "JEHOVAHSWITNESS":
                return Religion.JEHOVAHS_WITNESS;
            case "JEWISH":
                return Religion.JEWISH;
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
            case "OTHERCHRISTIANS":
                return Religion.CHRISTIAN;
            case "CHURCHOFGOD":
                return Religion.CHURCH_OF_GOD_OF_PROPHECY;
            case "OTHERSECTS/RELIGIONS":
                return null;
            case "ASSEMBLIESOFGOD":
                return null;
            case "CHURCHOFCHRIST":
                return null;
            case "EPISCOPAL":
                return null;
            case "NONE":
                return null;
            default:
                return null;   //allows free text only to be set
        }
    }
}
