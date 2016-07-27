package org.bdp.string_sim.utilities;

import java.io.File;

/**
 * Class for file name functions
 */
public class FileNameHelper {

    /**
     * Checks if a file with the desired file name already exists. If the file exists, the name will be extended by a number.
     * E.g. file name myOwnFile.csv exists --> myOwnFile1.csv will be returned
     *
     * @param desiredName the desired file name inclusive the path and extension
     * @param extension the file name extension in format ".csv"
     * @return a unique file name that not exist
     */
    public static String getUniqueFilename(String desiredName, String extension)
    {
        String prefix = desiredName.split(extension)[0];
        String uniqueName = desiredName;
        File outputFile = new File(desiredName);
        int i = 0;
        while (outputFile.exists()) {
            i++;
            uniqueName = prefix + Integer.toString(i) + extension;
            outputFile = new File(uniqueName);
        }
        return uniqueName;
    }
}
