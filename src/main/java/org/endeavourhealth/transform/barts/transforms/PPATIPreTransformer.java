package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.PPATI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPATIPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPATIPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch for record-level errors, as errors in this transform mean we can't continue
                createPatient((PPATI)parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createPatient(PPATI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell millenniumPersonIdCell = parser.getMillenniumPersonId();
        CsvCell mrnCell = parser.getLocalPatientId();

        if (mrnCell.isEmpty()) {
            return;
        }

        //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
        //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
        String localUniqueId = millenniumPersonIdCell.getString();
        String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrnCell.getString(); //this must match the HL7 Receiver
        String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
        csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Patient, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);

        //the problem file uses MRN as the patient identifier, so we need to store our PersonID - MRN mappings
        //in the internal ID table for future reference
        //store the MRN/PersonID mapping in BOTH directions
        csvHelper.saveInternalId(InternalIdMap.TYPE_MRN_TO_MILLENNIUM_PERSON_ID, mrnCell.getString(), millenniumPersonIdCell.getString());
        csvHelper.saveInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, millenniumPersonIdCell.getString(), mrnCell.getString());
    }
}

