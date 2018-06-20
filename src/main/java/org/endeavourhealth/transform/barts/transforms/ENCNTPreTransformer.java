package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ENCNTPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNTTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch here, because any error means it's not safe to continue
                processRecord((ENCNT)parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    /**
     * this pre-transformer tries to match the Data Warehouse Encounters to the HL7 Receiver Encounters
     * which needs the MRN and VISIT ID
     */
    public static void processRecord(ENCNT parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //in-active (i.e. deleted) rows don't have anything else but the ID, so we can't do anything with them
        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            return;
        }

        //NOTE the ENCNT transformer does ignore some records based on encounter type, but it doesn't hurt to still generate IDs for them

        //99%+ of ENCNT records have a VISIT ID, but some don't, so we can't use them
        CsvCell visitIdCell = parser.getVisitId();
        if (BartsCsvHelper.isEmptyOrIsZero(visitIdCell)) {
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //the HL7 Receiver uses the MRN as part of the Encounter ID, so we need to look that up
        String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personIdCell.getString());
        if (Strings.isNullOrEmpty(mrn)) {
            return;
        }

        //the Data Warehouse files all use PersonID as the unique local identifier for patients, but the
        //ADT feed uses the MRN, so we need to ensure that the Discovery UUID is the same as used by the ADT feed
        String localUniqueId = encounterIdCell.getString();
        String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrn + "-EpIdTypeCode=VISITID-EpIdValue=" + visitIdCell.getString(); //this must match the HL7 Receiver
        String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
        csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.Encounter, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);
    }

}
