/*
 *    Copyright 2021 Johannes Roesch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.github.johannesroesch.apollon.embedded;

import io.github.johannesroesch.apollon.exception.ApollonException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static java.lang.String.format;

public final class ValidationHelper {

    private ValidationHelper() {
    }

    public static void validateNotBlank(String arg, String message, Object... args) {
        if (StringUtils.isBlank(arg)) {
            throw new ApollonException(format(message, args));
        }
    }

    public static void validateNotNull(Object arg, String message, Object... args) {
        if (arg == null) {
            throw new ApollonException(format(message, args));
        }
    }

    public static void validateNotEmpty(Map<?, ?> arg, String message, Object... args) {
        if (MapUtils.isEmpty(arg)) {
            throw new ApollonException(format(message, args));
        }
    }

    public static void validateTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            throw new ApollonException(format(message, args));
        }
    }
}
