
package org.endeavourhealth.transform.emis.openhr.schema;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for voc.ReferralMode.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="voc.ReferralMode">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}token">
 *     &lt;enumeration value="E"/>
 *     &lt;enumeration value="T"/>
 *     &lt;enumeration value="V"/>
 *     &lt;enumeration value="W"/>
 *     &lt;enumeration value="M"/>
 *     &lt;enumeration value="C"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "voc.ReferralMode", namespace = "http://www.e-mis.com/emisopen")
@XmlEnum
public enum VocReferralMode {


    /**
     * Electronic
     * 
     */
    E,

    /**
     * Telephone
     * 
     */
    T,

    /**
     * Verbal
     * 
     */
    V,

    /**
     * Written
     * 
     */
    W,

    /**
     * Mediated
     * 
     */
    M,

    /**
     * WCCG
     * 
     */
    C;

    public String value() {
        return name();
    }

    public static VocReferralMode fromValue(String v) {
        return valueOf(v);
    }

}
