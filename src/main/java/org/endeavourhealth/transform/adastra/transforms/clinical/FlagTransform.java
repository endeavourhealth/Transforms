package org.endeavourhealth.transform.adastra.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.adastra.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.transforms.helpers.AdastraHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Period;

import java.util.Date;

public class FlagTransform {

    public static void transform(AdastraCaseDataExport.SpecialNote specialNote, String caseRef,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {
        Flag fhirFlag = new Flag();

        fhirFlag.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_FLAG));

        fhirFlag.setId(caseRef + ":" + specialNote.getText());

        fhirFlag.setSubject(AdastraHelper.createPatientReference());

        Date reviewDate = XmlDateHelper.convertDate(specialNote.getReviewDate());
        Period fhirPeriod = PeriodHelper.createPeriod(null, reviewDate);
        fhirFlag.setPeriod(fhirPeriod);

        CodeableConcept codeableConcept = new CodeableConcept();
        codeableConcept.setText(specialNote.getText());

        fhirFlag.setCode(codeableConcept);

        fhirResourceFiler.savePatientResource(null, fhirFlag);

    }
}
