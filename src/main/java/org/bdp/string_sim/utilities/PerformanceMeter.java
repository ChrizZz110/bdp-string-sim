package org.bdp.string_sim.utilities;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

/**
 * Performance Measuring class
 */
public class PerformanceMeter {
    private long startTimeMillis,
        endTimeMillis,
        diffTimeMillis;

    private String name;

    public PerformanceMeter(String name) {
        this.name = name;
    }

    public void startMeasuring() {
        this.startTimeMillis = System.currentTimeMillis();
    }

    public void stopMeasuring() {
        this.endTimeMillis = System.currentTimeMillis();
        this.diffTimeMillis = this.endTimeMillis - this.startTimeMillis;
    }

    public void writeResultsToConsole() {
        Handler ch = new ConsoleHandler();
        Logger.getLogger("").addHandler(ch);

        Logger.getLogger("").info("Performance Meter startet for Task '" + this.name + "'\n\n" +
                "Start: " + getTimeFromMillis(startTimeMillis) + "\n" +
                "End: " + getTimeFromMillis(endTimeMillis) + "\n" +
                "Duration: " + getTimeFromMillis(diffTimeMillis) + "\n");
    }

    private String getTimeFromMillis(long millis) {
        // New date object from millis
        Date date = new Date(millis);
        // formattter
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
        // Pass date object
        return formatter.format(date);
    }
}
