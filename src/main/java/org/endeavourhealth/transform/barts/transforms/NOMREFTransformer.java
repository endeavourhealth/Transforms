package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerNomenclatureRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.NOMREF;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class NOMREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(NOMREFTransformer.class);

    public static final String AUDIT_MNEMONIC_TEXT = "mnemonic_text";
    public static final String AUDIT_VALUE_TEXT = "value_text";
    public static final String AUDIT_DISPLAY_TEXT = "display_text";
    public static final String AUDIT_DESCRIPTION_TEXT = "description_text";
    public static final String AUDIT_CONCEPT_IDENTIFIER = "concept_identifier";

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<CernerNomenclatureRef> batch = new ArrayList<>();

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {

                //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
                //to parse any record in this file it a critical error
                transform((NOMREF) parser, fhirResourceFiler, csvHelper, batch);
            }
        }

        saveBatch(batch, true, csvHelper);
    }

    private static void saveBatch(List<CernerNomenclatureRef> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        csvHelper.submitToThreadPool(new SaveNomenclatureCallable(new ArrayList<>(batch)));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    public static void transform(NOMREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, List<CernerNomenclatureRef> batch) throws Exception {
        CsvCell idCell = parser.getNomenclatureId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell mnemonicTextCell = parser.getMnemonicText();
        CsvCell valueTextCell = parser.getValueText();
        CsvCell displayTextCell = parser.getDisplayText();
        CsvCell descriptionTextCell = parser.getDescriptionText();
        CsvCell typeCell = parser.getNomenclatureTypeCode();
        CsvCell vocabularyCodeCell = parser.getVocabularyCode();
        CsvCell conceptIdentifierCell = parser.getConceptIdentifer();

        CernerNomenclatureRef obj = new CernerNomenclatureRef();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();
        obj.setAudit(auditWrapper);

        //these cells can't be null or empty
        obj.setServiceId(fhirResourceFiler.getServiceId());
        obj.setNomenclatureId(idCell.getLong().longValue());
        obj.setActive(activeCell.getIntAsBoolean());

        if (!mnemonicTextCell.isEmpty()) {
            obj.setMnemonicText(mnemonicTextCell.getString());
            auditWrapper.auditValue(mnemonicTextCell.getPublishedFileId(), mnemonicTextCell.getRecordNumber(), mnemonicTextCell.getColIndex(), AUDIT_MNEMONIC_TEXT);
        }
        if (!valueTextCell.isEmpty()) {
            obj.setValueText(valueTextCell.getString());
            auditWrapper.auditValue(valueTextCell.getPublishedFileId(), valueTextCell.getRecordNumber(), valueTextCell.getColIndex(), AUDIT_VALUE_TEXT);
        }
        if (!displayTextCell.isEmpty()) {
            obj.setDisplayText(displayTextCell.getString());
            auditWrapper.auditValue(displayTextCell.getPublishedFileId(), displayTextCell.getRecordNumber(), displayTextCell.getColIndex(), AUDIT_DISPLAY_TEXT);
        }
        if (!descriptionTextCell.isEmpty()) {
            obj.setDescriptionText(descriptionTextCell.getString());
            auditWrapper.auditValue(descriptionTextCell.getPublishedFileId(), descriptionTextCell.getRecordNumber(), descriptionTextCell.getColIndex(), AUDIT_DESCRIPTION_TEXT);
        }
        if (!typeCell.isEmpty()) {
            obj.setNomenclatureTypeCode(typeCell.getLong());
            //not bothering to audit this field back to the source
        }
        if (!vocabularyCodeCell.isEmpty()) {
            obj.setVocabularyCode(vocabularyCodeCell.getLong());
            //not bothering to audit this field back to the source
        }
        if (!conceptIdentifierCell.isEmpty()) {
            obj.setConceptIdentifier(conceptIdentifierCell.getString());
            auditWrapper.auditValue(conceptIdentifierCell.getPublishedFileId(), conceptIdentifierCell.getRecordNumber(), conceptIdentifierCell.getColIndex(), AUDIT_CONCEPT_IDENTIFIER);
        }

        //spin the remainder of our work off to a small thread pool, so we can perform multiple snomed term lookups in parallel
        batch.add(obj);
        saveBatch(batch, false, csvHelper);
    }


    static class SaveNomenclatureCallable implements Callable {

        private List<CernerNomenclatureRef> objs = null;

        public SaveNomenclatureCallable(List<CernerNomenclatureRef> objs) {
            this.objs = objs;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveNOMREFs(objs);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
