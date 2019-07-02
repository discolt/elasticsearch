package org.elasticsearch.vpack.limit;

public class StringUtils {

    public static final int INDEX_NOT_FOUND = -1;

    public static int ordinalIndexOf(final CharSequence str, final CharSequence searchStr, final int ordinal) {
        return ordinalIndexOf(str, searchStr, ordinal, false);
    }

    private static int ordinalIndexOf(final CharSequence str, final CharSequence searchStr, final int ordinal, final boolean lastIndex) {
        if (str == null || searchStr == null || ordinal <= 0) {
            return INDEX_NOT_FOUND;
        }
        if (searchStr.length() == 0) {
            return lastIndex ? str.length() : 0;
        }
        int found = 0;
        int index = lastIndex ? str.length() : INDEX_NOT_FOUND;
        do {
            if (lastIndex) {
                index = lastIndexOf(str, searchStr, index - searchStr.length());
            } else {
                index = indexOf(str, searchStr, index + searchStr.length());
            }
            if (index < 0) {
                return index;
            }
            found++;
        } while (found < ordinal);
        return index;
    }

    static int lastIndexOf(final CharSequence cs, final CharSequence searchChar, final int start) {
        return cs.toString().lastIndexOf(searchChar.toString(), start);
    }

    static int indexOf(final CharSequence cs, final CharSequence searchChar, final int start) {
        return cs.toString().indexOf(searchChar.toString(), start);
    }
}
