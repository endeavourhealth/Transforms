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
        PatientBuilder patientBuilder = csvHelper.getPatientCache().borrowPatientBuilder(personIdCell, csvHelper);
        if (patientBuilder == null) {
            return;
        }


        //if our GP record is non-active or ended, we need to REMOVE the reference from our patient resource
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell endDateCell = parser.getEndEffectiveDate();
        boolean delete = !activeCell.getIntAsBoolean()
                || !BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell); //note that the Cerner end of time is used for active record end dates

        CsvCell personnelId = parser.getRegisteredGPMillenniumPersonnelId();
        if (!BartsCsvHelper.isEmptyOrIsZero(personnelId)) {

            Reference practitionerReference = csvHelper.createPractitionerReference(personnelId);
            if (patientBuilder.isIdMapped()) {
                practitionerReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }

            if (delete) {
                patientBuilder.removeCareProvider(practitionerReference);

            } else {
                patientBuilder.addCareProvider(practitionerReference);
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
                patientBuilder.addCareProvider(orgReference);
            }
        }

        csvHelper.getPatientCache().returnPatientBuilder(personIdCell, patientBuilder);
    }

}
