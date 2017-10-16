package org.endeavourhealth.transform.barts.schema;

public class TailsRecord {
    private String CDSUniqueueId;
    private String EncounterId;
    private String FINNbr;
    private String EpisodeId;

    public void setCDSUniqueueId(String CDSUniqueueId) {
        this.CDSUniqueueId = CDSUniqueueId;
    }

    public String getCDSUniqueueId() {
        return CDSUniqueueId;
    }

    public String getEncounterId() {
        return EncounterId;
    }

    public void setEncounterId(String encounterId) {
        EncounterId = encounterId;
    }

    public String getFINNbr() {
        return FINNbr;
    }

    public void setFINNbr(String FINNbr) {
        this.FINNbr = FINNbr;
    }

    public String getEpisodeId() {
        return EpisodeId;
    }

    public void setEpisodeId(String episodeId) {
        EpisodeId = episodeId;
    }
}
