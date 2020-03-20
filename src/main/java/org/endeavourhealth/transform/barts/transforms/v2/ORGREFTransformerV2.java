package org.endeavourhealth.transform.barts.transforms.v2;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.models.CoreFilerWrapper;
import org.endeavourhealth.core.database.dal.ehr.models.CoreId;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ORGREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class ORGREFTransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREFTransformerV2.class);

    private static CoreFilerDalI repository = DalProvider.factoryCoreFilerDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

        List<CoreFilerWrapper> batch = new ArrayList<>();

        for (ParserI parser : parsers) {
            while (parser.nextRecord()) {

                //no try/catch here, since any failure here means we don't want to continue
                createOrganization((ORGREF)parser, fhirResourceFiler, csvHelper, batch);
            }
        }
        saveBatch(batch, true, csvHelper);

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createOrganization(ORGREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, List<CoreFilerWrapper> batch) throws Exception {

        org.endeavourhealth.core.database.dal.ehr.models.Organization organization
                = new org.endeavourhealth.core.database.dal.ehr.models.Organization();

        CsvCell orgIdCell = parser.getOrgId();
        CoreId orgId = csvHelper.getCoreId(CoreTableId.ORGANIZATION.getId(), orgIdCell.getString());
        organization.setId(orgId.getCoreId());

        CsvCell orgNameCell = parser.getOrgNameText();
        if (!orgNameCell.isEmpty()) {
            organization.setName(orgNameCell.getString());
        }

        CsvCell orgAliasCell = parser.getNhsOrgAlias(); //ODS code
        if (!orgAliasCell.isEmpty()) {
            organization.setOdsCode(orgAliasCell.getString());
        }

        CsvCell parentOrgAliasCell = parser.getParentNhsOrgAlias();
        if (!parentOrgAliasCell.isEmpty()) {
            //confusingly, the file links orgs to its parents using the ODS code, not the ID, so we need
            //to look up the ID for our parent using the ODS code
            String parentOdsCode = parentOrgAliasCell.getString();

            //there are some records that have themselves as their parent, so
            //check for this and ignore that
            String odsCode = orgAliasCell.getString();
            if (odsCode == null
                || !parentOdsCode.equals(odsCode)) {

                String parentId = csvHelper.getInternalId(InternalIdMap.TYPE_CERNER_ODS_CODE_TO_ORG_ID, parentOdsCode);
                if (!Strings.isNullOrEmpty(parentId)) {

                    CoreId parentOrgId = csvHelper.getCoreId(CoreTableId.ORGANIZATION.getId(), parentId);
                    organization.setParentOrganizationId(parentOrgId.getCoreId());
                }
            }
        }

        //TODO: set the IM reference based on type
        organization.setTypeId(-1);

        CsvCell orgPostCodeCell = parser.getPostCodeTxt();
        if (!orgPostCodeCell.isEmpty()) {

            String postCode = orgPostCodeCell.getString();
            postCode = postCode.substring(0, 10);  //some invalid long non post codes, so trim to 10.
            organization.setPostCode(postCode);
        }

        //create the CoreFilerWrapper for filing
        CoreFilerWrapper coreFilerWrapper = new CoreFilerWrapper();
        coreFilerWrapper.setServiceId(csvHelper.getServiceId());
        coreFilerWrapper.setSystemId(csvHelper.getSystemId());
        coreFilerWrapper.setDeleted(false);
        coreFilerWrapper.setCreatedAt(new Date());
        coreFilerWrapper.setExchangeId(csvHelper.getExchangeId());
        coreFilerWrapper.setDataType(CoreTableId.ORGANIZATION.getName());
        coreFilerWrapper.setData(organization);
        batch.add(coreFilerWrapper);

        saveBatch(batch, false, csvHelper);
    }

    private static void saveBatch(List<CoreFilerWrapper> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new ORGREFTransformerV2.saveDataCallable(new ArrayList<>(batch), serviceId));
        batch.clear();

        if (lastOne) {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static class saveDataCallable implements Callable {

        private List<CoreFilerWrapper> objs = null;
        private UUID serviceId;

        public saveDataCallable(List<CoreFilerWrapper> objs,
                                UUID serviceId) {
            this.objs = objs;
            this.serviceId = serviceId;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.save(serviceId, objs);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
