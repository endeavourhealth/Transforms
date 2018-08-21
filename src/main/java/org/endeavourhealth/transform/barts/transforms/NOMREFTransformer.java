package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerNomenclatureRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.NOMREF;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NOMREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(CVREFTransformer.class);

    public static final String AUDIT_MNEMONIC_TEXT = "mnemonic_text";
    public static final String AUDIT_VALUE_TEXT = "value_text";
    public static final String AUDIT_DISPLAY_TEXT = "display_text";
    public static final String AUDIT_DESCRIPTION_TEXT = "description_text";
    public static final String AUDIT_CONCEPT_IDENTIFIER = "concept_identifier";

    private static CernerCodeValueRefDalI repository = DalProvider.factoryCernerCodeValueRefDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            for (ParserI parser: parsers) {

                while (parser.nextRecord()) {

                    //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
                    //to parse any record in this file it a critical error
                    transform((NOMREF) parser, fhirResourceFiler, csvHelper);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }


    public static void transform(NOMREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {
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
            auditWrapper.auditValue(mnemonicTextCell.getRowAuditId(), mnemonicTextCell.getColIndex(), AUDIT_MNEMONIC_TEXT);
        }
        if (!valueTextCell.isEmpty()) {
            obj.setValueText(valueTextCell.getString());
            auditWrapper.auditValue(valueTextCell.getRowAuditId(), valueTextCell.getColIndex(), AUDIT_VALUE_TEXT);
        }
        if (!displayTextCell.isEmpty()) {
            obj.setDisplayText(displayTextCell.getString());
            auditWrapper.auditValue(displayTextCell.getRowAuditId(), displayTextCell.getColIndex(), AUDIT_DISPLAY_TEXT);
        }
        if (!descriptionTextCell.isEmpty()) {
            obj.setDescriptionText(descriptionTextCell.getString());
            auditWrapper.auditValue(descriptionTextCell.getRowAuditId(), descriptionTextCell.getColIndex(), AUDIT_DESCRIPTION_TEXT);
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
            auditWrapper.auditValue(conceptIdentifierCell.getRowAuditId(), conceptIdentifierCell.getColIndex(), AUDIT_CONCEPT_IDENTIFIER);
        }

        //spin the remainder of our work off to a small thread pool, so we can perform multiple snomed term lookups in parallel
        csvHelper.submitToThreadPool(new SaveNomenclatureCallable(parser.getCurrentState(), obj));
    }


    static class SaveNomenclatureCallable extends AbstractCsvCallable {

        private CernerNomenclatureRef obj = null;

        public SaveNomenclatureCallable(CsvCurrentState parserState,
                                        CernerNomenclatureRef obj) {
            super(parserState);
            this.obj = obj;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveNomenclatureRef(obj);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
