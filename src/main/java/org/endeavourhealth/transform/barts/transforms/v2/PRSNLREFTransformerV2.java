package org.endeavourhealth.transform.barts.transforms.v2;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.models.CoreFilerWrapper;
import org.endeavourhealth.core.database.dal.ehr.models.CoreId;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PRSNLREF;
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

import static org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE;

public class PRSNLREFTransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREFTransformerV2.class);

    public static final String MAPPING_ID_PERSONNEL_NAME_TO_ID = "PersonnelNameToId";
    public static final String MAPPING_ID_CONSULTANT_TO_ID = "ConsultantToId";

    private static CoreFilerDalI repository = DalProvider.factoryCoreFilerDal();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        if (TransformConfig.instance().isLive()) {
            //remove this check for go live
            return;
        }

        List<CoreFilerWrapper> batch = new ArrayList<>();

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createPractitioner((PRSNLREF) parser, fhirResourceFiler, csvHelper, batch);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
        saveBatch(batch, true, csvHelper);

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createPractitioner(PRSNLREF parser,
                                           FhirResourceFiler fhirResourceFiler,
                                           BartsCsvHelper csvHelper,
                                           List<CoreFilerWrapper> batch) throws Exception {

        org.endeavourhealth.core.database.dal.ehr.models.Practitioner practitioner
                = new org.endeavourhealth.core.database.dal.ehr.models.Practitioner();

        CsvCell personnelIdCell = parser.getPersonnelID();
        CoreId personnelId = csvHelper.getCoreId(CoreTableId.PRACTITIONER.getId(), personnelIdCell.getString());
        practitioner.setId(personnelId.getCoreId());

        //use the Barts ODS code to lookup the OrganizationId from the Core DB or Cached
        Integer organizationId = csvHelper.findOrganizationIdForOds(PRIMARY_ORG_ODS_CODE);
        if (organizationId != null) {
            practitioner.setOrganizationId(organizationId);
        }

        String fullName = parser.getFullFormatName().getString().trim();
        practitioner.setName(fullName);

        CsvCell positionCodeCell = parser.getMilleniumPositionCode();
        if (!BartsCsvHelper.isEmptyOrIsZero(positionCodeCell)) {

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.PERSONNEL_POSITION, positionCodeCell);
            if (cernerCodeValueRef != null) {
                practitioner.setRoleCode(cernerCodeValueRef.getAliasNhsCdAlias());
                practitioner.setRoleDesc(cernerCodeValueRef.getCodeDescTxt());
            }
        }

        //TODO: IM mapping for practitioner type, from what?
        practitioner.setTypeId(-1);

        //create the CoreFilerWrapper for filing
        CoreFilerWrapper coreFilerWrapper = new CoreFilerWrapper();
        coreFilerWrapper.setServiceId(csvHelper.getServiceId());
        coreFilerWrapper.setSystemId(csvHelper.getSystemId());
        coreFilerWrapper.setDeleted(false);
        coreFilerWrapper.setCreatedAt(new Date());
        coreFilerWrapper.setExchangeId(csvHelper.getExchangeId());
        coreFilerWrapper.setDataType(CoreTableId.PRACTITIONER.getName());
        coreFilerWrapper.setData(practitioner);
        batch.add(coreFilerWrapper);

        saveBatch(batch, false, csvHelper);
    }

    private static void saveBatch(List<CoreFilerWrapper> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new PRSNLREFTransformerV2.saveDataCallable(new ArrayList<>(batch), serviceId));
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
