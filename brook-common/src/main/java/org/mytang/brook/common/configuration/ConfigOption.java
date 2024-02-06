package org.mytang.brook.common.configuration;

import org.mytang.brook.common.configuration.description.Description;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ConfigOption<T> {
    private static final FallbackKey[] EMPTY = new FallbackKey[0];

    static final Description EMPTY_DESCRIPTION = Description.builder().text("").build();

    // ------------------------------------------------------------------------

    /**
     * The current key for that config option.
     */
    private final String key;

    /**
     * The list of deprecated keys, in the order to be checked.
     */
    private final FallbackKey[] fallbackKeys;

    /**
     * The default value for this option.
     */
    private final T defaultValue;

    /**
     * The description for this option.
     */
    private final Description description;

    /**
     * Type of the value that this ConfigOption describes.
     * <ul>
     *     <li>typeClass == atomic class (e.g. {@code Integer.class}) -> {@code ConfigOption<Integer>}</li>
     *     <li>typeClass == {@code Map.class} -> {@code ConfigOption<Map<String, String>>}</li>
     *     <li>typeClass == atomic class and isList == true for {@code ConfigOption<List<Integer>>}</li>
     * </ul>
     */
    private final Class<?> clazz;

    private final boolean isList;

    public Class<?> getClazz() {
        return clazz;
    }

    public boolean isList() {
        return isList;
    }

    /**
     * Creates a new config option with fallback keys.
     *
     * @param key          The current key for that config option
     * @param clazz        describes type of the ConfigOption, see description of the clazz field
     * @param description  Description for that option
     * @param defaultValue The default value for this option
     * @param isList       tells if the ConfigOption describes a list option, see description of the clazz field
     * @param fallbackKeys The list of fallback keys, in the order to be checked
     */
    ConfigOption(
            String key,
            Class<?> clazz,
            Description description,
            T defaultValue,
            boolean isList,
            FallbackKey... fallbackKeys) {
        this.key = Objects.requireNonNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
        this.clazz = Objects.requireNonNull(clazz);
        this.isList = isList;
    }

    /**
     * Creates a new config option, using this option's key and default value, and
     * adding the given fallback keys.
     *
     * <p>When obtaining a value from the configuration via {@link Configuration#getValue(ConfigOption)},
     * the fallback keys will be checked in the order provided to this method. The first key for which
     * a value is found will be used - that value will be returned.
     *
     * @param fallbackKeys The fallback keys, in the order in which they should be checked.
     * @return A new config options, with the given fallback keys.
     */
    public ConfigOption<T> withFallbackKeys(String... fallbackKeys) {
        final Stream<FallbackKey> newFallbackKeys = Arrays
                .stream(fallbackKeys)
                .map(FallbackKey::createFallbackKey);
        final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

        // put fallback keys first so that they are prioritized
        final FallbackKey[] mergedAlternativeKeys = Stream
                .concat(newFallbackKeys, currentAlternativeKeys)
                .toArray(FallbackKey[]::new);
        return new ConfigOption<>(
                key,
                clazz,
                description,
                defaultValue,
                isList,
                mergedAlternativeKeys);
    }

    /**
     * Creates a new config option, using this option's key and default value, and
     * adding the given deprecated keys.
     *
     * @param deprecatedKeys The deprecated keys, in the order in which they should be checked.
     * @return A new config options, with the given deprecated keys.
     */
    public ConfigOption<T> withDeprecatedKeys(String... deprecatedKeys) {
        final Stream<FallbackKey> newDeprecatedKeys = Arrays
                .stream(deprecatedKeys)
                .map(FallbackKey::createDeprecatedKey);
        final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

        // put deprecated keys last so that they are de-prioritized
        final FallbackKey[] mergedAlternativeKeys = Stream
                .concat(currentAlternativeKeys, newDeprecatedKeys)
                .toArray(FallbackKey[]::new);
        return new ConfigOption<>(
                key,
                clazz,
                description,
                defaultValue,
                isList,
                mergedAlternativeKeys);
    }

    /**
     * Creates a new config option, using this option's key and default value, and
     * adding the given description. The given description is used when generation the configuration documention.
     *
     * @param description The description for this option.
     * @return A new config option, with given description.
     */
    public ConfigOption<T> withDescription(final String description) {
        return withDescription(Description.builder().text(description).build());
    }

    /**
     * Creates a new config option, using this option's key and default value, and
     * adding the given description. The given description is used when generation the configuration documention.
     *
     * @param description The description for this option.
     * @return A new config option, with given description.
     */
    public ConfigOption<T> withDescription(final Description description) {
        return new ConfigOption<>(key, clazz, description, defaultValue, isList, fallbackKeys);
    }

    // ------------------------------------------------------------------------

    /**
     * Gets the configuration key.
     *
     * @return The configuration key
     */
    public String key() {
        return key;
    }

    /**
     * Checks if this option has a default value.
     *
     * @return True if it has a default value, false if not.
     */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /**
     * Returns the default value, or null, if there is no default value.
     *
     * @return The default value, or null.
     */
    public T defaultValue() {
        return defaultValue;
    }

    /**
     * Checks whether this option has deprecated keys.
     *
     * @return True if the option has deprecated keys, false if not.
     * @deprecated Replaced by {@link #hasFallbackKeys()}
     */
    @Deprecated
    public boolean hasDeprecatedKeys() {
        return fallbackKeys != EMPTY && Arrays
                .stream(fallbackKeys)
                .anyMatch(FallbackKey::isDeprecated);
    }

    /**
     * Gets the deprecated keys, in the order to be checked.
     *
     * @return The option's deprecated keys.
     * @deprecated Replaced by {@link #fallbackKeys()}
     */
    @Deprecated
    public Iterable<String> deprecatedKeys() {
        return fallbackKeys == EMPTY ? Collections.emptyList() :
                Arrays.stream(fallbackKeys)
                        .filter(FallbackKey::isDeprecated)
                        .map(FallbackKey::getKey)
                        .collect(Collectors.toList());
    }

    /**
     * Checks whether this option has fallback keys.
     *
     * @return True if the option has fallback keys, false if not.
     */
    public boolean hasFallbackKeys() {
        return fallbackKeys != EMPTY;
    }

    /**
     * Gets the fallback keys, in the order to be checked.
     *
     * @return The option's fallback keys.
     */
    public Iterable<FallbackKey> fallbackKeys() {
        return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
    }

    /**
     * Returns the description of this option.
     *
     * @return The option's description.
     */
    public Description description() {
        return description;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == ConfigOption.class) {
            ConfigOption<?> that = (ConfigOption<?>) o;
            return this.key.equals(that.key) &&
                    Arrays.equals(this.fallbackKeys, that.fallbackKeys) &&
                    (this.defaultValue == null ? that.defaultValue == null :
                            (that.defaultValue != null
                                    && this.defaultValue.equals(that.defaultValue)));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() +
                17 * Arrays.hashCode(fallbackKeys) +
                (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format("Key: '%s' , default: %s (fallback keys: %s)",
                key, defaultValue, Arrays.toString(fallbackKeys));
    }
}
