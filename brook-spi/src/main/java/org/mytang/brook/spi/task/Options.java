package org.mytang.brook.spi.task;

import org.mytang.brook.common.constants.Delimiter;
import org.mytang.brook.common.configuration.ConfigOption;
import org.mytang.brook.common.configuration.Configuration;

import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface Options {
    @SuppressWarnings("unchecked")
    Class<Map<String, Object>> PROPERTIES_MAP_CLASS =
            (Class<Map<String, Object>>) (Class<?>) Map.class;

    /**
     * the catalog is options set archive directory
     *
     * @return catalog
     */
    @NotNull ConfigOption<?> catalog();

    /**
     * required options set
     *
     * @return set
     */
    default Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    /**
     * optional options set
     *
     * @return set
     */
    default Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    /**
     * all options set
     *
     * @return set
     */
    default Set<ConfigOption<?>> allOptions() {
        Set<ConfigOption<?>> allOptions = new HashSet<>();
        Optional
                .ofNullable(requiredOptions())
                .filter(set -> !set.isEmpty())
                .ifPresent(allOptions::addAll);

        Optional
                .ofNullable(optionalOptions())
                .filter(set -> !set.isEmpty())
                .ifPresent(allOptions::addAll);
        return allOptions;
    }

    /**
     * verify options, universal check
     *
     * @param configuration configuration
     */
    default void verify(@NotNull Configuration configuration) {
        StringBuilder verifyResult = new StringBuilder();

        Set<ConfigOption<?>> allOptions = allOptions();

        Map<String, ConfigOption<?>> allOptionsMap = allOptions.stream()
                .collect(Collectors.toMap(ConfigOption::key, v -> v));

        // illegal check, if it exists, end early
        configuration.keySet().stream()
                .filter(suspectedKey -> suspectedKey.charAt(0) != Delimiter.UNDER_LINE.charAt(0)
                        && allOptionsMap.get(suspectedKey) == null)
                .forEach(suspectedKey ->
                        verifyResult.append(
                                String.format("illegal option: %s\n", suspectedKey)));
        if (verifyResult.length() > 0) {
            throw new ValidationException(
                    String.format("'%s' appears illegal options:\n%s",
                            catalog().key(), verifyResult));
        }

        // required check
        requiredOptions().stream()
                .filter(option -> !configuration.contains(option))
                .forEach(option ->
                        verifyResult.append(
                                String.format("miss option: %s\n", option.key()))
                );

        // parsable check
        allOptions.forEach(option -> {
            try {
                configuration.get(option);
            } catch (Exception e) {
                verifyResult.append(e.getLocalizedMessage()).append("\n");
            }
        });

        try {
            doVerify(configuration);
        } catch (Exception e) {
            verifyResult.append(e.getLocalizedMessage());
        }

        if (verifyResult.length() > 0) {
            throw new ValidationException(
                    String.format("'%s' options:\n%s",
                            catalog().key(), verifyResult));
        }
    }

    /**
     * verify options, personalized check
     *
     * @param configuration configuration
     */
    default void doVerify(@NotNull Configuration configuration) {

    }

    /**
     * refresh configuration
     * <p>
     * E.g: useful for flushing with JVM local cache
     *
     * @param configuration configuration
     */
    default void refresh(@NotNull Configuration configuration) {

    }
}
