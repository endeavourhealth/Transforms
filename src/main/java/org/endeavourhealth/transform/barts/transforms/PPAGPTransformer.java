package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPAGP;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPAGPTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPAGPTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createPatientGP((PPAGP)parser, fhirResourceFiler, csvHelper);
            }
        }
    }


    public static void createPatientGP(PPAGP parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell relationshipTypeCell = parser.getPersonPersonnelRelationCode();

        //SD-295, we've had a tiny number of cases where we've had PPAGP records with type "0". Looking at the raw data,
        //the patients appear to have a separate valid record with a valid type, so the "0" record can be ignored
        if (BartsCsvHelper.isEmptyOrIsZero(relationshipTypeCell)) {
            return;
        }

        //the relation code links to the standard code ref table, and defines the type of relationship
        //we're only interested in who the patients registered GP is
        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICIAL_RELATIONSHIP_TYPE, relationshipTypeCell);
        String display = codeRef.getCodeDispTxt();
        if (!display.equalsIgnoreCase("Registered GP")) {
            return;
            //throw new TransformException("PPAGP record has unexpected relation code " + relationshipType.getLong());
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //we get no person ID when it's a delete, but we also get a replacement record for the person, adding the n
        //new GP details, which will replace what's on the resource. So just return out here.
        if (personIdCell.isEmpty()) {
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            return;
        }

        try {
            //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
            CsvCell activeCell = parser.getActiveIndicator();
            CsvCell endDateCell = parser.getEndEffectiveDate();

            //Barts send multiple records per patient, with the end date and active indicator telling us which are active or not
            boolean isActive = parser.getActiveIndicator().getIntAsBoolean()
                    && BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell);

            //if this record isn't active, ignore it. A separate record should also be present to give us the
            //current details, which will be processed.
            if (!isActive) {
                return;
            }

            //remove any existing care providers, since we only support having one current org and one current practitioner
            patientBuilder.removeAllCareProviders();

            //add the GP, if present
            CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
            if (!BartsCsvHelper.isEmptyOrIsZero(personnelId)) {

                Reference practitionerReference = csvHelper.createPractitionerReference(personnelId);
                if (patientBuilder.isIdMapped()) {
                    practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
                }

                patientBuilder.addCareProvider(practitionerReference, personnelId);
            }

            //add the GP practice
            CsvCell orgIdCell = parser.getRegisteredGPPracticeMillenniumIdOrganisationCode();
            if (!BartsCsvHelper.isEmptyOrIsZero(orgIdCell)) {

                Reference orgReference = csvHelper.createOrganizationReference(orgIdCell);
                if (patientBuilder.isIdMapped()) {
                    orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, csvHelper);
                }

                patientBuilder.addCareProvider(orgReference, orgIdCell);
            }

        } finally {
            csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
        }
    }

    /*public static void createPatientGP(PPAGP parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //the relation code links to the standard code ref table, and defines the type of relationship
        //we're only interested in who the patients registered GP is
        CsvCell relationshipType = parser.getPersonPersonnelRelationCode();
        if (!relationshipType.isEmpty()) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICIAL_RELATIONSHIP_TYPE, relationshipType);
            if (codeRef != null) {
                String display = codeRef.getCodeDispTxt();
                if (!display.equalsIgnoreCase("Registered GP")) {
                    return;
                    //throw new TransformException("PPAGP record has unexpected relation code " + relationshipType.getLong());
                }
            }
        }

        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //we get no person ID when it's a delete, but we also get a replacement record for the person, adding the n
        //new GP details, which will replace what's on the resource. So just return out here.
        if (personIdCell.isEmpty()) {
            return;
        }

        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell);
        if (patientBuilder == null) {
            return;
        }

        //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell endDateCell = parser.getEndEffectiveDate();
        boolean delete = !activeCell.getIntAsBoolean()
                || !BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell); //note that the Cerner end of time is used for active record end dates

        //if this record is adding an active GP/practice, then remove any existing references from the resource
        if (!delete) {
            patientBuilder.removeAllCareProviders();
        }

        CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelId)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(personnelId);
            if (patientBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }

            if (delete) {
                patientBuilder.removeCareProvider(practitionerReference);

            } else {
                patientBuilder.addCareProvider(practitionerReference, personnelId);
            }
        }

        CsvCell orgIdCell = parser.getRegisteredGPPracticeMillenniumIdOrganisationCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(orgIdCell)) {

            Reference orgReference = csvHelper.createOrganizationReference(orgIdCell);
            if (patientBuilder.isIdMapped()) {
                orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, csvHelper);
            }

            if (delete) {
                patientBuilder.removeCareProvider(orgReference);

            } else {
                patientBuilder.addCareProvider(orgReference, orgIdCell);
            }
        }

        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }*/

}
