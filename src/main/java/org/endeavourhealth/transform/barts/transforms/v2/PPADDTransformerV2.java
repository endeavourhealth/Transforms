package org.endeavourhealth.transform.barts.transforms.v2;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.CoreFilerDalI;
import org.endeavourhealth.core.database.dal.ehr.models.CoreFilerWrapper;
import org.endeavourhealth.core.database.dal.ehr.models.CoreId;
import org.endeavourhealth.core.database.dal.ehr.models.CoreTableId;
import org.endeavourhealth.core.database.dal.ehr.models.PatientAddress;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCodeableConceptHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.schema.PPADD;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer.PRIMARY_ORG_ODS_CODE;

public class PPADDTransformerV2 {
    private static final Logger LOG = LoggerFactory.getLogger(PPADDTransformerV2.class);

    private static final String EMAIL_ID_PREFIX = "PPADD";

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

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                processRecord((PPADD) parser, fhirResourceFiler, csvHelper, batch);
            }
        }
        saveBatch(batch, true, csvHelper);

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void processRecord(PPADD parser,
                                     FhirResourceFiler fhirResourceFiler,
                                     BartsCsvHelper csvHelper,
                                     List<CoreFilerWrapper> batch) throws Exception {


        PatientAddress patientAddress = new PatientAddress();

        CsvCell addressIdCell = parser.getMillenniumAddressId();
        CoreId corePatientAddressId = csvHelper.getCoreId(CoreTableId.PATIENT_ADDRESS.getId(), addressIdCell.getString());
        patientAddress.setId(corePatientAddressId.getCoreId());

        CsvCell personIdCell = parser.getPersonId();
        CoreId corePatientId = csvHelper.getCoreId(CoreTableId.PATIENT.getId(), personIdCell.getString());
        patientAddress.setPatientId(corePatientId.getCoreId());

        //if non-active (i.e. deleted) we should REMOVE the address from the patient
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            //There are a small number of cases where all the fields are empty (including person ID) but in all examined
            //cases, we've never previously received a valid record, so can just ignore them
            if (!personIdCell.isEmpty()) {

                //TODO:  a deletion on patient_address

            }
            return;
        }

        //SD-295 - there are a small number (about 100 out of millions) of PPADD records where the type code is zero. The vast majority
        //of these have empty address fields, and the remainder are garage (e.g. city = "Z999"), so ignore any record like this
        CsvCell typeCell = parser.getAddressTypeCode();
        if (BartsCsvHelper.isEmptyOrIsZero(typeCell)) {
            //there's no point logging this now since there's no further investigation that will be done
            //TransformWarnings.log(LOG, csvHelper, "Skipping PPADD {} for person {} because it has no type", addressIdCell, personIdCell);
            return;
        }


        CsvCell line1 = parser.getAddressLine1();
        CsvCell line2 = parser.getAddressLine2();
        CsvCell line3 = parser.getAddressLine3();
        CsvCell line4 = parser.getAddressLine4();
        CsvCell city = parser.getCity();
        CsvCell postcode = parser.getPostcode();

        //ignore any empty records
        if (line1.isEmpty()
                && line2.isEmpty()
                && line3.isEmpty()
                && line4.isEmpty()
                && city.isEmpty()
                && postcode.isEmpty()) {
            return;
        }

        //use the Barts ODS code to lookup the OrganizationId from the Core DB or Cached
        Integer organizationId = csvHelper.findOrganizationIdForOds(PRIMARY_ORG_ODS_CODE);
        if (organizationId != null) {
            patientAddress.setOrganizationId(organizationId);
        }

        patientAddress.setAddressLine1(line1.getString());
        patientAddress.setAddressLine2(line2.getString());
        patientAddress.setAddressLine3(line3.getString());
        patientAddress.setAddressLine4(line4.getString());
        patientAddress.setCity(city.getString());
        patientAddress.setPostCode(postcode.getString());

        boolean isActive = true;
        CsvCell startDate = parser.getBeginEffectiveDate();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(startDate)) {

            Date sd = BartsCsvHelper.parseDate(startDate);
            patientAddress.setStartDate(sd);
            isActive = true;
        }
        CsvCell endDate = parser.getEndEffectiveDate();
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(endDate)) {

            Date ed = BartsCsvHelper.parseDate(endDate);
            patientAddress.setEndDate(ed);
            isActive = false;
        }

        //TODO:  map IM address type Id (home, temp, business etc)
        CsvCell typeDescCell = BartsCodeableConceptHelper.getCellDesc(csvHelper, CodeValueSet.ADDRESS_TYPE, typeCell);
        String typeDesc = typeDescCell.getString();
        Address.AddressUse use = convertAddressUse(typeDesc, isActive);
        if (use == Address.AddressUse.HOME) {
            //Cache the current address Id for filing against the patient
            csvHelper.cachePatientCurrentAddressId(corePatientId.getCoreId(), corePatientAddressId.getCoreId());
        }
        patientAddress.setUseTypeId(-1);

        //TODO:  how do we map the msoa, lsoa and ward / local authority codes?

        //create the CoreFilerWrapper for filing
        CoreFilerWrapper coreFilerWrapper = new CoreFilerWrapper();
        coreFilerWrapper.setServiceId(csvHelper.getServiceId());
        coreFilerWrapper.setSystemId(csvHelper.getSystemId());
        coreFilerWrapper.setDeleted(false);
        coreFilerWrapper.setCreatedAt(new Date());
        coreFilerWrapper.setExchangeId(csvHelper.getExchangeId());
        coreFilerWrapper.setDataType(CoreTableId.PATIENT_ADDRESS.getName());
        coreFilerWrapper.setData(patientAddress);
        batch.add(coreFilerWrapper);

        saveBatch(batch, false, csvHelper);
    }

    private static Address.AddressType convertAddressType(String typeDesc) throws TransformException {

        //NOTE we only use address type if it's explicitly known to be a mailing address
        if (typeDesc.equalsIgnoreCase("mailing")) {
            return Address.AddressType.POSTAL;

        } else if (typeDesc.equalsIgnoreCase("Birth Address")
                || typeDesc.equalsIgnoreCase("home")
                || typeDesc.equalsIgnoreCase("business")
                || typeDesc.equalsIgnoreCase("temporary")
                || typeDesc.equalsIgnoreCase("Prevous Address") //note the wrong spelling is in the Cerner data CVREF file
                || typeDesc.equalsIgnoreCase("Alternate Address")
        ) {
            return null;

        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressUse(..) too
            throw new TransformException("Unhandled type [" + typeDesc + "]");
        }
    }

    private static Address.AddressUse convertAddressUse(String typeDesc, boolean isActive) throws TransformException {

        //FHIR states to use "old" for anything no longer active
        if (!isActive) {
            return Address.AddressUse.OLD;
        }

        //NOTE there are 20+ address types in CVREF, but only the types known to be used are mapped below
        if (typeDesc.equalsIgnoreCase("Birth Address")
                || typeDesc.equalsIgnoreCase("home")
                || typeDesc.equalsIgnoreCase("mailing")) {
            return Address.AddressUse.HOME;

        } else if (typeDesc.equalsIgnoreCase("business")) {
            return Address.AddressUse.WORK;

        } else if (typeDesc.equalsIgnoreCase("temporary")
                || typeDesc.equalsIgnoreCase("Alternate Address")) {
            return Address.AddressUse.TEMP;

        } else if (typeDesc.equalsIgnoreCase("Prevous Address")) { //note the wrong spelling is in the Cerner data CVREF file
            return Address.AddressUse.OLD;

        } else {
            //NOTE if adding a new type above here make sure to add to convertAddressType(..) too
            throw new TransformException("Unhandled type [" + typeDesc + "]");
        }
    }

    private static void saveBatch(List<CoreFilerWrapper> batch, boolean lastOne, BartsCsvHelper csvHelper) throws Exception {

        if (batch.isEmpty()
                || (!lastOne && batch.size() < TransformConfig.instance().getResourceSaveBatchSize())) {
            return;
        }

        UUID serviceId = csvHelper.getServiceId();
        csvHelper.submitToThreadPool(new PPADDTransformerV2.saveDataCallable(new ArrayList<>(batch), serviceId));
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