package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherCommon.TppImmunisationContentDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppImmunisationContent;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRImmunisationContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRImmunisationContentTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRImmunisationContentTransformer.class);

    private static TppImmunisationContentDalI repository = DalProvider.factoryTppImmunisationContentDal();
    public static final String ROW_ID = "RowId";
    public static final String NAME = "name";
    public static final String CONTENT = "content";
    public static final String DATE_DELETED = "dateDeleted";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRImmunisationContent.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRImmunisationContent) parser, fhirResourceFiler);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(SRImmunisationContent parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell name = parser.getName();
        CsvCell content = parser.getContent();
        CsvCell dateDeleted = parser.getDateDeleted();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getRowAuditId(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(name.getRowAuditId(), name.getColIndex(), NAME);
        auditWrapper.auditValue(content.getRowAuditId(), content.getColIndex(), CONTENT);
        auditWrapper.auditValue(dateDeleted.getRowAuditId(), dateDeleted.getColIndex(), DATE_DELETED);

        TppImmunisationContent mapping = new TppImmunisationContent(rowId.getLong(),
                name.getString(),
                content.getString(),
                dateDeleted.getDate(),
                auditWrapper);


        //save to the DB
        repository.save(mapping);

    }
}
