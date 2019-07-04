package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPRELPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPRELPreTransformer.class);

    //public static final String PPREL_ID_TO_PERSON_ID = "PPREL_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPREL)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    public static void processRecord(PPREL parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //all this pre-transformer does is quickly store ID -> person ID mappings so the person can be later looked
        //up if this record is ever deleted, in which case the record doesn't contain the person ID
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        if (!parser.getMillenniumPersonIdentifier().isEmpty() && !parser.getRelatedPersonMillenniumIdentifier().isEmpty()) {
            CsvCell relatedPersonIdCell = parser.getRelatedPersonMillenniumIdentifier();
            CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
            CsvCell relationshipToPatientCodeCell = parser.getRelationshipToPatientCode();

            PPRELPreTransformCallable callable = new PPRELPreTransformCallable(parser.getCurrentState(), relatedPersonIdCell, personIdCell, relationshipToPatientCodeCell, csvHelper);
            csvHelper.submitToThreadPool(callable);
        }
    }


    static class PPRELPreTransformCallable extends AbstractCsvCallable {

        private CsvCell relatedPersonIdCell = null;
        private CsvCell personIdCell = null;
        private CsvCell relationshipToPatientCodeCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPRELPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell relatedPersonIdCell,
                                         CsvCell personIdCell,
                                         CsvCell relationshipToPatientCodeCell,
                                         BartsCsvHelper csvHelper) {

            super(parserState);
            this.relatedPersonIdCell = relatedPersonIdCell;
            this.personIdCell = personIdCell;
            this.relationshipToPatientCodeCell = relationshipToPatientCodeCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPREL ID -> PERSON ID mapping so that if the record is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                //not required - when we get a non-active PPREL record, we still get the person ID, so don't need this mapping
                /*String relatedPersonId = relatedPersonIdCell.getString();
                String personId = personIdCell.getString();
                String existingPersonId = csvHelper.getInternalId(PPREL_ID_TO_PERSON_ID, relatedPersonId);
                if (existingPersonId == null
                        || !existingPersonId.equals(personId)) {

                    csvHelper.saveInternalId(PPREL_ID_TO_PERSON_ID, relatedPersonId, personId);
                }*/

                //store the relationship type in the internal ID map table so the FamilyHistoryTransformer can look it up
                csvHelper.savePatientRelationshipType(personIdCell, relatedPersonIdCell, relationshipToPatientCodeCell);

                //pre-cache the patient resource
                csvHelper.getPatientCache().preCachePatientBuilder(personIdCell);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


