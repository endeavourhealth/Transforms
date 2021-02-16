package org.endeavourhealth.transform.hl7v2fhir.transforms;

import ca.uhn.hl7v2.model.v23.datatype.TS;
import ca.uhn.hl7v2.model.v23.segment.PV1;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EpisodeOfCareTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareTransformer.class);

    /**
     *
     * @param pv1
     * @param episodeOfCare
     * @return
     * @throws Exception
     */
    public static EpisodeOfCareBuilder transformPV1ToEpisodeOfCare(PV1 pv1, EpisodeOfCareBuilder episodeOfCare) throws Exception {
        
        // episodeOfCare.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_EPISODE_OF_CARE));

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCare);
        identifierBuilder.setSystem("");
        identifierBuilder.setValue(pv1.getVisitNumber().getID().toString());

        episodeOfCare.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        TS admitDtTime = pv1.getAdmitDateTime();
        String startDt = String.valueOf(admitDtTime.getTimeOfAnEvent());
        if(startDt != null) {
            Date stDt = formatter.parse(startDt.substring(0, 4) + "-" + startDt.substring(4, 6) + "-" + startDt.substring(6, 8));
            episodeOfCare.setRegistrationStartDate(stDt);
        }

        TS dischargeDtTime = pv1.getDischargeDateTime();
        String endDt = String.valueOf(dischargeDtTime.getTimeOfAnEvent());
        if(endDt != null) {
            Date dsDt = formatter.parse(endDt.substring(0,4)+"-"+endDt.substring(4,6)+"-"+endDt.substring(6,8));
            episodeOfCare.setRegistrationEndDate(dsDt);
        }

        return episodeOfCare;
    }

}
