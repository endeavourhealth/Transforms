package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.BlobContent;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BlobContentPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(BlobContentPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {


                    //no try/catch as failures here meant we should abort. No patient filtering possible on this file
                    processRecord((BlobContent)parser, fhirResourceFiler, csvHelper);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(BlobContent parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //we need to store a cache of blob Document (224) data against the CLEVE event_id, only if non empty and not DELETED
        //so we can retrieve it for IM mapping during the CLEVEPreTransform of Document clinical events
        CsvCell blobClassRefCell = parser.getBlobClassRef();
        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.CLINICAL_EVENT_CLASS, blobClassRefCell);
        String classDesc = codeRef.getCodeDescTxt();
        if (!classDesc.equalsIgnoreCase("Document")) {
            return;
        }

        CsvCell eventIdCell = parser.getEventId();
        CsvCell blobContentsCell = parser.getBlobContents();
        if (!blobContentsCell.isEmpty()
                && !blobContentsCell.getString().equalsIgnoreCase("DELETED")) {

            BlobContentPreTransformCallable callable
                    = new BlobContentPreTransformCallable(parser.getCurrentState(), eventIdCell, blobContentsCell, csvHelper);
            csvHelper.submitToThreadPool(callable);
        }
    }


    static class BlobContentPreTransformCallable extends AbstractCsvCallable {

        private CsvCell eventIdCell = null;
        private CsvCell blobContentsCell = null;
        private BartsCsvHelper csvHelper = null;

        public BlobContentPreTransformCallable(CsvCurrentState parserState,
                                               CsvCell eventIdCell,
                                               CsvCell blobContentsCell,
                                               BartsCsvHelper csvHelper) {
            super(parserState);

            this.eventIdCell = eventIdCell;
            this.blobContentsCell = blobContentsCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //pre-cache the event blob contents
                csvHelper.cacheEventIdBlobContent(eventIdCell, blobContentsCell);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


