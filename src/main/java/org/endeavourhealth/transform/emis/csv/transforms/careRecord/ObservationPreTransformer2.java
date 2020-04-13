package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.helpers.*;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Observation;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * ensures that all UserInRoleGUIDs in the file are cached in the admin helper so we know
 * which staff are actually referenced by clinical data
 */
public class ObservationPreTransformer2 {
    private static final Logger LOG = LoggerFactory.getLogger(ObservationPreTransformer2.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        Observation parser = (Observation) parsers.get(Observation.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    CsvCell clinicianCell = parser.getClinicianUserInRoleGuid();
                    csvHelper.getAdminHelper().addRequiredUserInRole(clinicianCell);

                    CsvCell enteredCell = parser.getEnteredByUserInRoleGuid();
                    csvHelper.getAdminHelper().addRequiredUserInRole(enteredCell);
                }

            } catch (Exception ex) {
                //because this is a pre-transformer to cache data, throw any exception so we don't continue
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }
}


