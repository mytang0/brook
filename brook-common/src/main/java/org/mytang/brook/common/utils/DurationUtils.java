package org.mytang.brook.common.utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class DurationUtils {

    private static final Map<String, ChronoUnit> LABEL_TO_UNIT_MAP;

    static {
        Map<String, ChronoUnit> labelToUnit = new HashMap<>();
        for (InternalTimeUnit timeUnit : InternalTimeUnit.values()) {
            for (String label : timeUnit.getLabels()) {
                labelToUnit.put(label, timeUnit.getUnit());
            }
        }
        LABEL_TO_UNIT_MAP = labelToUnit;
    }

    /**
     * Parse the given string to a java {@link Duration}.
     * The string is in format "{length value}{time unit label}", e.g. "123ms", "321 s".
     * If no time unit label is specified, it will be considered as milliseconds.
     *
     * <p>Supported time unit labels are:
     * <ul>
     *     <li>DAYS： "d", "day"</li>
     *     <li>HOURS： "h", "hour"</li>
     *     <li>MINUTES： "min", "minute"</li>
     *     <li>SECONDS： "s", "sec", "second"</li>
     *     <li>MILLISECONDS： "ms", "milli", "millisecond"</li>
     *     <li>MICROSECONDS： "µs", "micro", "microsecond"</li>
     *     <li>NANOSECONDS： "ns", "nano", "nanosecond"</li>
     * </ul>
     *
     * @param text string to parse.
     */
    public static Duration parseDuration(String text) {
        requireNonNull(text);

        final String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("argument is an empty- or whitespace-only string");
        }

        final int len = trimmed.length();
        int pos = 0;

        char current;
        while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
            pos++;
        }

        final String number = trimmed.substring(0, pos);
        final String unitLabel = trimmed.substring(pos).trim().toLowerCase(Locale.US);

        if (number.isEmpty()) {
            throw new NumberFormatException("text does not start with a number");
        }

        final long value;
        try {
            // this throws a NumberFormatException on overflow
            value = Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The value '" + number +
                    "' cannot be re represented as 64bit number (numeric overflow).");
        }

        if (unitLabel.isEmpty()) {
            return Duration.of(value, ChronoUnit.MILLIS);
        }

        ChronoUnit unit = LABEL_TO_UNIT_MAP.get(unitLabel);
        if (unit != null) {
            return Duration.of(value, unit);
        } else {
            throw new IllegalArgumentException("Time interval unit label '" + unitLabel +
                    "' does not match any of the recognized units: "
                    + InternalTimeUnit.getAllUnits());
        }
    }

    /**
     * Enum which defines time unit, mostly used to parse value from configuration file.
     */
    private enum InternalTimeUnit {

        DAYS(ChronoUnit.DAYS, singular("d"), plural("day")),
        HOURS(ChronoUnit.HOURS, singular("h"), plural("hour")),
        MINUTES(ChronoUnit.MINUTES, singular("min"), plural("minute")),
        SECONDS(ChronoUnit.SECONDS, singular("s"), plural("sec"), plural("second")),
        MILLISECONDS(ChronoUnit.MILLIS, singular("ms"), plural("milli"), plural("millisecond")),
        MICROSECONDS(ChronoUnit.MICROS, singular("µs"), plural("micro"), plural("microsecond")),
        NANOSECONDS(ChronoUnit.NANOS, singular("ns"), plural("nano"), plural("nanosecond"));

        private static final String PLURAL_SUFFIX = "s";

        private final List<String> labels;

        private final ChronoUnit unit;

        InternalTimeUnit(ChronoUnit unit, String[]... labels) {
            this.unit = unit;
            this.labels = Arrays
                    .stream(labels)
                    .flatMap(Arrays::stream)
                    .collect(Collectors.toList());
        }

        /**
         * @param label the original label
         * @return the singular format of the original label
         */
        private static String[] singular(String label) {
            return new String[]{
                    label
            };
        }

        /**
         * @param label the original label
         * @return both the singular format and plural format of the original label
         */
        private static String[] plural(String label) {
            return new String[]{
                    label,
                    label + PLURAL_SUFFIX
            };
        }

        public List<String> getLabels() {
            return labels;
        }

        public ChronoUnit getUnit() {
            return unit;
        }

        public static String getAllUnits() {
            return Arrays.stream(InternalTimeUnit.values())
                    .map(InternalTimeUnit::createTimeUnitString)
                    .collect(Collectors.joining(", "));
        }

        private static String createTimeUnitString(InternalTimeUnit timeUnit) {
            return timeUnit.name() + ": (" + String.join(" | ", timeUnit.getLabels()) + ")";
        }
    }
}
