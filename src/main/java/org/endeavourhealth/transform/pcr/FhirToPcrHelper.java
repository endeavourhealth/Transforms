package org.endeavourhealth.transform.pcr;

import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;

/**
 * Utility class for various generic methods
 */
public class FhirToPcrHelper {


    public static void freeTextWriter(long textId, int patientId, String freeText,
                                      AbstractPcrCsvWriter csvWriter) throws Exception {
//  Isn't a transformer in its own right. Called by transformers to add FreeText
        // May need to return id?
        // If we need a transformer for some data sources we can just write a wrapper for this method.
        // TODO check if it needs to return Id. Esp if we use same id map technique


        //    Long textId =  AbstractTransformer.findOrCreatePcrId(params, resourceType.toString(), resourceId.toString());

        org.endeavourhealth.transform.pcr.outputModels.FreeText model
                = (org.endeavourhealth.transform.pcr.outputModels.FreeText) csvWriter;
        model.writeUpsert(textId,
                patientId,
                freeText);
    }


}
