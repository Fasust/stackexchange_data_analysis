package utility;

import java.util.*;

public class CountryValidator {

    private ArrayList<String> countries;

    public CountryValidator(){
        countries = new ArrayList<>();

        for (String iso : Locale.getISOCountries()) {
            Locale l = new Locale("", iso);

            countries.add(l.getDisplayCountry().toLowerCase());
        }
    }

    /**
     * Check if String is the name of a ISO Country
     * @param loc potential Country
     * @return
     */
    public boolean isCountry(String loc){
        return countries.contains(loc);
    }
}
