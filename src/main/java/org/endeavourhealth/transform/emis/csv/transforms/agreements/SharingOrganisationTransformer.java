package org.endeavourhealth.transform.emis.csv.transforms.agreements;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.agreements.SharingOrganisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SharingOrganisationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SharingOrganisationTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        SharingOrganisation parser = (SharingOrganisation)parsers.get(SharingOrganisation.class);

        //we don't actually generate any FHIR resources from this file, but check to make sure
        //that the sharing agreement hasn't been disabled. If it has, then we throw an exception
        //and fail the transform. This will mean that all the patient deletes aren't processed
        //and no further messages for this organisation will be processed either. When Emis
        //fix the problem then this will need removing to allow those messages to be processed.
        parser.nextRecord();

        CsvCell disabled = parser.getDisabled();
        if (disabled.getBoolean()) {

            //if we've already decided we don't want to process any patient data, then we don't care what the state
            //of the sharing agreement is
            if (csvHelper.isProcessPatientData()) {

                ServiceDalI serviceDal = DalProvider.factoryServiceDal();
                Service service = serviceDal.getById(csvHelper.getServiceId());
                String odsCode = service.getLocalId();

                boolean allowed = false;
                List<Pattern> disabledOrgIdsAllowed = TransformConfig.instance().getEmisDisabledOragnisationsAllowed();
                for (Pattern pattern : disabledOrgIdsAllowed) {
                    Matcher matcher = pattern.matcher(odsCode);
                    if (matcher.matches()) {
                        allowed = true;
                        break;
                    }
                }

                if (!allowed) {
                    throw new TransformException("Not processing Exchange because org disabled in sharing agreements file");
                }
            }
        }
    }


}
