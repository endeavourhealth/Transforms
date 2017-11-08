package org.endeavourhealth.transform.adastra;

import org.endeavourhealth.common.utility.XmlHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.schema.CodedItem;
import org.endeavourhealth.transform.adastra.transforms.admin.LocationTransform;
import org.endeavourhealth.transform.adastra.transforms.admin.OrganisationTransformer;
import org.endeavourhealth.transform.adastra.transforms.admin.PatientTransformer;
import org.endeavourhealth.transform.adastra.transforms.clinical.EncounterTransform;
import org.endeavourhealth.transform.adastra.transforms.clinical.EpisodeTransformer;
import org.endeavourhealth.transform.adastra.transforms.clinical.FlagTransform;
import org.endeavourhealth.transform.adastra.transforms.clinical.ObservationTransformer;
import org.hl7.fhir.instance.model.Resource;

import java.util.ArrayList;
import java.util.List;

public class AdastraXmlToFhirTransformer {

    public static List<Resource> toFhirFullRecord(String xmlPayload) throws Exception {

        //TODO - use XSD to validate received XML

        AdastraCaseDataExport caseReport = XmlHelper.deserialize(xmlPayload, AdastraCaseDataExport.class);

        List<Resource> ret = new ArrayList<>();

        //TODO - handle case reference
        //TODO - handle care number

        OrganisationTransformer.transform(caseReport, ret);
        LocationTransform.transform(caseReport, ret);
        PatientTransformer.transform(caseReport, ret);
        EpisodeTransformer.transform(caseReport, ret);
        EncounterTransform.createMainCaseEncounter(caseReport, ret);

        if (caseReport.getOutcome() != null) {
            for (CodedItem codedItem : caseReport.getOutcome()) {
                ObservationTransformer.observationFromCodedItem(codedItem, "caseEncounter", caseReport.getActiveDate(), ret);
            }
        }

        if (caseReport.getPresentingCondition() != null)
            ObservationTransformer.observationFromPresentingCondition(caseReport, ret);

        if  (caseReport.getQuestions() != null && !caseReport.getQuestions().isEmpty()) {
            ObservationTransformer.observationFromFreeText(caseReport.getQuestions(), caseReport.getAdastraCaseReference(), caseReport.getActiveDate(), ret);
        }

        if (caseReport.getSpecialNote() != null) {
            for (AdastraCaseDataExport.SpecialNote specialNote : caseReport.getSpecialNote()) {
                FlagTransform.transform(specialNote, "caseEncounter", ret);
            }
        }

        return ret;
    }
}
