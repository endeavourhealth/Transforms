package org.endeavourhealth.transform.barts.transforms.v2;

import org.endeavourhealth.core.database.dal.ehr.models.CoreId;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPAGP;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPAGPTransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PPAGPTransformerV2.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

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

        //we get no person ID when it's a delete, but we also get a replacement record for the person, adding the n
        //new GP details, which will replace what's on the resource. So just return out here.
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        if (personIdCell.isEmpty()) {
            return;
        }

        //get new/cached/from db Patient
        org.endeavourhealth.core.database.dal.ehr.models.Patient patient =  csvHelper.getPatientCache().borrowPatientV2Instance(personIdCell);
        if (patient == null) {
            return;
        }

        try {

            //Cerner allows multiple GP records for a patient, but in all cases examined we only have one active row
            //so ignore any ended or non-active records and simply let the active record overwrite the care provider each time
            CsvCell activeCell = parser.getActiveIndicator();
            CsvCell endDateCell = parser.getEndEffectiveDate();
            boolean isActive = !activeCell.getIntAsBoolean()
                    || !BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell); //note that the Cerner end of time is used for active record end dates

            if (isActive) {

                //add the GP practice
                CsvCell orgIdCell = parser.getRegisteredGPPracticeMillenniumIdOrganisationCode();
                if (!BartsCsvHelper.isEmptyOrIsZero(orgIdCell)) {

                    CoreId orgId = csvHelper.getCoreId(CoreTableId.ORGANIZATION.getId(), orgIdCell.getString());
                    patient.setRegisteredPracticeOrganizationId(orgId.getCoreId());
                }
            }

        } finally {
            csvHelper.getPatientCache().returnPatientV2Instance(personIdCell, patient);
        }
    }
}
