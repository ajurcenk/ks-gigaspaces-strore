package org.ajur.demo.kstreams.giigaspaces.store.gks;

public class Utils {

    public static String convertToValidJavaName(String s){
        StringBuilder sb = new StringBuilder();
        if(!Character.isJavaIdentifierStart(s.charAt(0))) {
            sb.append("_");
        }
        for (char c : s.toCharArray()) {
            if(!Character.isJavaIdentifierPart(c)) {
                sb.append("_");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}

