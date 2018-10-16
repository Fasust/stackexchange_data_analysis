package utility;

import java.util.*;

public final class CountryValidator {

    private static final Set<String> COUNTRIES = new HashSet<String> (Arrays.asList(Locale.getISOCountries()));

    /**
     * Check if String is the name of a ISO Country
     * @param loc potential Country
     * @return
     */
    public static boolean isCountry(String loc){
        return COUNTRIES.contains(loc);
    }
}
