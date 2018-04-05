package org.endeavourhealth.transform.tpp.csv.transforms.codes;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.TppConfigListOptionDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.tpp.csv.schema.codes.SRConfiguredListOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRConfiguredListOptionTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRConfiguredListOptionTransformer.class);

    private static TppConfigListOptionDalI repository = DalProvider.factoryTppConfigListOptionDal();
    public static final String ROW_ID = "RowId";
    public static final String CONFIG_LIST_ID = "ConfigListId";
    public static final String LIST_OPTION = "ListOption";

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        AbstractCsvParser parser = parsers.get(SRConfiguredListOption.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRConfiguredListOption)parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    public static void createResource(SRConfiguredListOption parser, FhirResourceFiler fhirResourceFiler) throws Exception {

        CsvCell rowId = parser.getRowIdentifier();
        CsvCell configListId = parser.getConfiguredList();
        CsvCell listOption = parser.getConfiguredListOption();

        ResourceFieldMappingAudit auditWrapper = new ResourceFieldMappingAudit();

        auditWrapper.auditValue(rowId.getRowAuditId(), rowId.getColIndex(), ROW_ID);
        auditWrapper.auditValue(configListId.getRowAuditId(), configListId.getColIndex(), CONFIG_LIST_ID);
        auditWrapper.auditValue(listOption.getRowAuditId(), listOption.getColIndex(), LIST_OPTION);

        TppConfigListOption mapping = new TppConfigListOption(rowId.getLong(),
                                    configListId.getLong(),
                                    listOption.getString(),
                                    fhirResourceFiler.getServiceId().toString(),
                                    auditWrapper);


        //save to the DB
        repository.save(mapping, fhirResourceFiler.getServiceId());

    }
}
