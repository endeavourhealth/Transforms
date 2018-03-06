package org.endeavourhealth.transform.common;

import java.util.UUID;

public interface HasServiceSystemAndExchangeIdI {

    UUID getServiceId();
    UUID getSystemId();
    UUID getExchangeId();
}
