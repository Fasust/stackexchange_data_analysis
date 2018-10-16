package utility;

import java.util.*;

public class CountryValidator {

    private ArrayList<String> countryNames;
    private ArrayList<String> countryCodes;

    public CountryValidator(){
        countryCodes = new ArrayList<>();
        countryNames = new ArrayList<>();

        for (String iso : Locale.getISOCountries()) {
            Locale l = new Locale("", iso);

            countryNames.add(l.getDisplayCountry().toLowerCase());
            countryCodes.add(iso.toLowerCase());
        }
    }

    /**
     * Check if String is the name of a ISO Country
     * Or if the String is a Country code
     * @param loc potential Country
     * @return boolean
     */
    public boolean isCountry(String loc){
        return countryNames.contains(loc) || countryCodes.contains(loc);
    }
}
