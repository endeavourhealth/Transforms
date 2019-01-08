package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.common.CsvCell;

/**
 * object used to cache SUS tail records in memory. Although the tail files are slightly different between
 * Inpatient, Outpatient and Emergency, they're the same as far as the fields we're interested in go
 */
public class SusTailCacheEntry {
    private CsvCell finNumber;
    private CsvCell encounterId;
    private CsvCell episodeId;
    private CsvCell personId;
    private CsvCell responsibleHcpPersonnelId;

    public CsvCell getFinNumber() {
        return finNumber;
    }

    public void setFinNumber(CsvCell finNumber) {
        this.finNumber = finNumber;
    }

    public CsvCell getEncounterId() {
        return encounterId;
    }

    public void setEncounterId(CsvCell encounterId) {
        this.encounterId = encounterId;
    }

    public CsvCell getEpisodeId() {
        return episodeId;
    }

    public void setEpisodeId(CsvCell episodeId) {
        this.episodeId = episodeId;
    }

    public CsvCell getPersonId() {
        return personId;
    }

    public void setPersonId(CsvCell personId) {
        this.personId = personId;
    }

    public CsvCell getResponsibleHcpPersonnelId() {
        return responsibleHcpPersonnelId;
    }

    public void setResponsibleHcpPersonnelId(CsvCell responsibleHcpPersonnelId) {
        this.responsibleHcpPersonnelId = responsibleHcpPersonnelId;
    }
}
