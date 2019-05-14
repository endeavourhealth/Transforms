package org.endeavourhealth.transform.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BstHelper {
    private static final Logger LOG = LoggerFactory.getLogger(BstHelper.class);

    private static List<Calendar> calendarCache = new ArrayList<>();
    private static Map<Integer, Date> bstStarts = new ConcurrentHashMap<>();
    private static Map<Integer, Date> bstEnds = new ConcurrentHashMap<>();

    public static boolean isBst(Date d) {
        Calendar cal = borrowCalendar();
        try {
            cal.setTime(d);

            int year = cal.get(Calendar.YEAR);
            Date bstStart = getBstStart(year);
            Date bstEnd = getBstEnd(year);

            //all the below code is based on all dates just being in the current timezone,
            //so validate that the date passed in is also in the current timezone otherwise
            //we'll need to make this smarter to handle the differences between them
            String dZone = cal.getTimeZone().getID();
            String currentZone = TimeZone.getDefault().getID();
            if (!dZone.equals(currentZone)) {
                throw new RuntimeException("BST Helper doesn't support non-default timezones (" + dZone + ") when checking (current is " + currentZone + ")");
            }

            //start date is inclusive, end date is exclusive
            return !d.before(bstStart) && d.before(bstEnd);

        } finally {
            returnCalendar(cal);
        }

    }

    public static void testBsts() {
        for (int i=1995; i<2025; i++) {
            Date start = getBstStart(i);
            Date end = getBstEnd(i);

            LOG.debug("" + i + " BST from " + start + " to " + end);
        }
    }

    private static Date getBstStart(int year) {
        Date d = bstStarts.get(new Integer(year));
        if (d == null) {
            //BST starts at 01:00 UTC on the last Sunday in March
            Calendar cal = borrowCalendar();
            try {
                //set to the 1st April
                cal.clear();
                //cal.setTimeZone(TimeZone.getTimeZone("UTC")); //the below is in terms of UTC
                cal.set(Calendar.YEAR, year);
                cal.set(Calendar.MONTH, Calendar.APRIL);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.HOUR_OF_DAY, 1);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);

                //roll backward until we find a Sunday
                while (true) {
                    cal.add(Calendar.DAY_OF_YEAR, -1);

                    int day = cal.get(Calendar.DAY_OF_WEEK);
                    if (day == Calendar.SUNDAY) {
                        break;
                    }
                }

                d = cal.getTime();
                bstStarts.put(new Integer(year), d);
            } finally {
                returnCalendar(cal);
            }
        }
        return d;
    }

    private static Date getBstEnd(int year) {
        Date d = bstEnds.get(new Integer(year));
        if (d == null) {
            //BST end at 01:00 UTC (i.e. 02:00 BST) on the last Sunday in October
            Calendar cal = borrowCalendar();
            try {
                //set to the 1st April
                cal.clear();
                //cal.setTimeZone(TimeZone.getTimeZone("UTC")); //the below is in terms of UTC
                cal.set(Calendar.YEAR, year);
                cal.set(Calendar.MONTH, Calendar.NOVEMBER);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.HOUR_OF_DAY, 1);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);

                //roll backward until we find a Sunday
                while (true) {
                    cal.add(Calendar.DAY_OF_YEAR, -1);

                    int day = cal.get(Calendar.DAY_OF_WEEK);
                    if (day == Calendar.SUNDAY) {
                        break;
                    }
                }

                d = cal.getTime();
                bstEnds.put(new Integer(year), d);
            } finally {
                returnCalendar(cal);
            }
        }
        return d;
    }


    /**
     * use a cache of Calendars because creating them is pretty expensive
     */
    public static Calendar borrowCalendar() {

        synchronized (calendarCache) {
            if (calendarCache.isEmpty()) {
                return Calendar.getInstance();
            } else {
                return calendarCache.remove(0);
            }
        }
    }

    public static void returnCalendar(Calendar cal) {
        synchronized (calendarCache) {
            calendarCache.add(cal);
        }
    }
}
