package org.endeavourhealth.transform.homerton.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.homerton.HomertonCodeableConceptHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.endeavourhealth.transform.homerton.cache.PatientResourceCache;
import org.endeavourhealth.transform.homerton.schema.PatientTable;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PatientTransformer extends HomertonBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientTransformer.class);

    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper,
                                 String primaryOrgOdsCode) throws Exception {

        for (ParserI parser: parsers) {
            if (parser != null) {
                while (parser.nextRecord()) {
                    try {
                        transform((PatientTable) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        }
    }

    public static void transform(PatientTable parser,
                                             FhirResourceFiler fhirResourceFiler,
                                             HomertonCsvHelper csvHelper) throws Exception {

        CsvCell millenniumPersonIdCell = parser.getPersonId();
        CsvCell cnnCell = parser.getCNN();

        //store the MRN/PersonID mapping in BOTH directions
        csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, cnnCell.getString(), millenniumPersonIdCell.getString());
        csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, millenniumPersonIdCell.getString(), cnnCell.getString());

        PatientBuilder patientBuilder = PatientResourceCache.getPatientBuilder(millenniumPersonIdCell, csvHelper);

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID);
        identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
        identifierBuilder.setValue(cnnCell.getString(), cnnCell);

        patientBuilder.setActive(true);

        CsvCell firstNameCell = parser.getFirstname();
        CsvCell lastNameCell = parser.getSurname();

        NameBuilder nameBuilder = new NameBuilder(patientBuilder);
        nameBuilder.setUse(HumanName.NameUse.OFFICIAL);
        nameBuilder.addGiven(firstNameCell.getString(), firstNameCell);
        nameBuilder.addFamily(lastNameCell.getString(), lastNameCell);

        // Telecom
        CsvCell mobileCell = parser.getMobileTel();
        if (!mobileCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.MOBILE);
            contactPointBuilder.setValue(mobileCell.getString(), mobileCell);
        }

        CsvCell homeCell = parser.getHomeTel();
        if (!homeCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
            contactPointBuilder.setValue(homeCell.getString(), homeCell);
        }

        CsvCell workCell = parser.getWorkTel();
        if (!workCell.isEmpty()) {
            ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);
            contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.PHONE);
            contactPointBuilder.setUse(ContactPoint.ContactPointUse.WORK);
            contactPointBuilder.setValue(workCell.getString(), workCell);
        }

        // Gender
        CsvCell genderCell = parser.getGenderID();
        if (!genderCell.isEmpty()) {
            Enumerations.AdministrativeGender fhirGender = convertGenderToFHIR(genderCell.getInt().intValue());
            patientBuilder.setGender(fhirGender, genderCell);
        }

        // Ethnic group
        CsvCell ethnicityIdCell = parser.getEthnicGroupID();
        if (!csvHelper.isEmptyOrIsZero(ethnicityIdCell)) {

            CernerCodeValueRef codeRef = csvHelper.lookUpCernerCodeFromCodeSet(CodeValueSet.ETHNIC_GROUP, ethnicityIdCell.getString());
            if (codeRef == null) {
                TransformWarnings.log(LOG, parser, "ERROR: cerner code {} for ethnicity {} not found",
                        ethnicityIdCell.getLong(), parser.getEthnicGroupName().getString());

            } else {
                String codeDesc = codeRef.getAliasNhsCdAlias();
                if (!Strings.isNullOrEmpty(codeDesc)) {
                    EthnicCategory ethnicCategory = convertEthnicCategory(codeDesc);
                    patientBuilder.setEthnicity(ethnicCategory, ethnicityIdCell);
                } else {
                    TransformWarnings.log(LOG, parser, "ERROR: cerner code {} for ethnicity {} cannot be mapped with blank NHS Alias Code",
                            ethnicityIdCell.getLong(), parser.getEthnicGroupName().getString());
                    patientBuilder.setEthnicity(null);
                }
            }
        } else {
            //if this field is empty we should clear the value from the patient
            patientBuilder.setEthnicity(null);
        }


        // Date of birth
        CsvCell dobCell = parser.getDOB();
        if (!dobCell.isEmpty()) {
            patientBuilder.setDateOfBirth(dobCell.getDateTime(), dobCell);
        }

        CsvCell dodCell = parser.getDOD();
        if (!dodCell.isEmpty()) {
            patientBuilder.setDateOfDeath(dodCell.getDateTime(), dobCell);
        }

        // GP
        CsvCell gpCell = parser.getGPID();
        if (!gpCell.isEmpty() && gpCell.getString().length() > 0) {
            ResourceId resourceId = getGPResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, gpCell.getString());
            if (resourceId == null) {
                resourceId = createGPResourceId(HomertonCsvToFhirTransformer.HOMERTON_RESOURCE_ID_SCOPE, gpCell.getString());
            }
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, resourceId.getResourceId().toString());
            patientBuilder.addCareProvider(practitionerReference, gpCell);
        }

        // GP Practice
        CsvCell practiceCell = parser.getPracticeID();
        if (!practiceCell.isEmpty() && practiceCell.getString().length() > 0) {
            ResourceId resourceId = getGlobalOrgResourceId(practiceCell.getString());
            if (resourceId == null) {
                resourceId = createGlobalOrgResourceId(practiceCell.getString());
            }
            Reference organisationReference = ReferenceHelper.createReference(ResourceType.Organization, resourceId.getResourceId().toString());
            patientBuilder.addCareProvider(organisationReference, practiceCell);
        }

        // Address
        CsvCell line1Cell = parser.getAddressLine1();
        CsvCell line2Cell = parser.getAddressLine2();
        CsvCell line3Cell = parser.getAddressLine3();
        CsvCell cityCell = parser.getCity();
        CsvCell countyCell = parser.getCounty();
        CsvCell postcodeCell = parser.getPostcode();

        AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
        addressBuilder.setUse(Address.AddressUse.HOME);
        addressBuilder.addLine(line1Cell.getString(), line1Cell);
        addressBuilder.addLine(line2Cell.getString(), line2Cell);
        addressBuilder.addLine(line3Cell.getString(), line3Cell);
        addressBuilder.setTown(cityCell.getString(), cityCell);
        addressBuilder.setDistrict(countyCell.getString(), countyCell);
        addressBuilder.setPostcode(postcodeCell.getString(), postcodeCell);

        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, null);
        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Religion, null);

        CsvCell languageCell = parser.getLanguageID();
        HomertonCodeableConceptHelper.applyCodeDescTxt(languageCell, CodeValueSet.LANGUAGE, patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, csvHelper);

        CsvCell religionCell = parser.getReligionID();
        HomertonCodeableConceptHelper.applyCodeDescTxt(religionCell, CodeValueSet.RELIGION, patientBuilder, CodeableConceptBuilder.Tag.Patient_Religion, csvHelper);


        if (LOG.isTraceEnabled()) {
            LOG.trace("Save PatientTable:" + FhirSerializationHelper.serializeResource(patientBuilder.getResource()));
        }
    }

    /*
     *
     */
    public static Enumerations.AdministrativeGender convertGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.FEMALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.MALE;
            } else {
                if (gender == 9) {
                    return Enumerations.AdministrativeGender.NULL;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
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
