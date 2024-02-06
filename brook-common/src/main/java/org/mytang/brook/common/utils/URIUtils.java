package org.mytang.brook.common.utils;

import org.mytang.brook.common.constants.Delimiter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public abstract class URIUtils {

    public static Map<String, String> getParams(URI uri) {
        Map<String, String> params = new HashMap<>();
        String query = uri.getQuery();
        if (query == null || query.length() == 0) {
            return params;
        }

        String[] pairs = query.split(Delimiter.AND);
        if (pairs.length < 1) {
            return params;
        }

        for (String param : pairs) {
            String[] nameValue = param.split(Delimiter.EQUAL);
            if (nameValue.length == 2) {
                params.put(nameValue[0], nameValue[1]);
            } else if (nameValue.length == 1) {
                params.put(nameValue[0], null);
            }
        }
        return params;
    }
}
