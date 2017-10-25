package org.endeavourhealth.transform.emis.reverseCsv.transforms;

import org.endeavourhealth.common.cache.ParserPool;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.transform.ResourceIdTransformDalI;
import org.endeavourhealth.core.database.dal.transform.models.ResourceIdMap;
import org.endeavourhealth.transform.common.AbstractCsvWriter;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;

public abstract class AbstractTransformer {

    private ResourceIdTransformDalI idMapRepository = DalProvider.factoryResourceIdTransformDal();
    protected static final ParserPool PARSER_POOL = new ParserPool();

    public void transform(ResourceWrapper resourceWrapper, Map<Class, AbstractCsvWriter> writers) throws Exception {

        //find the source local ID for our EDS ID
        ResourceIdMap obj = idMapRepository.getResourceIdMapByEdsId(resourceWrapper.getResourceType(), resourceWrapper.getResourceId());
        String sourceId = obj.getSourceId();

        if (resourceWrapper.isDeleted()) {

            transformDeleted(sourceId, writers);

        } else {

            String json = resourceWrapper.getResourceData();
            Resource resource = PARSER_POOL.parse(json);
            transform(resource, sourceId, writers);
        }
    }

    protected abstract void transform(Resource resource, String sourceId, Map<Class, AbstractCsvWriter> writers) throws Exception;
    protected abstract void transformDeleted(String sourceId, Map<Class, AbstractCsvWriter> writers) throws Exception;

}
