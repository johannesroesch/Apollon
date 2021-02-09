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
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertThrows;

public class ValidationHelperTest {
    public static final String MESSAGE_TEMPLATE = "message %s";
    public static final String MESSAGE_EXPECTED = "message 1";

    @Test
    public void testPositiveValidateNotBlank() {
        ValidationHelper.validateNotBlank("notBlank", MESSAGE_TEMPLATE, 1);
    }

    @Test
    public void testNegativeValidateNotBlank() {
        assertThrows(MESSAGE_EXPECTED, ApollonException.class, () -> ValidationHelper.validateNotBlank("", MESSAGE_TEMPLATE, 1));
    }

    @Test
    public void testPositiveValidateNotNull() {
        ValidationHelper.validateNotNull("notNull", MESSAGE_TEMPLATE, 1);
    }

    @Test
    public void testNegativeValidateNotNull() {
        assertThrows(MESSAGE_EXPECTED, ApollonException.class, () -> ValidationHelper.validateNotNull(null, MESSAGE_TEMPLATE, 1));
    }

    @Test
    public void testPositiveValidateNotEmpty() {
        ValidationHelper.validateNotEmpty(Collections.singletonMap("key", "value"), MESSAGE_TEMPLATE, 1);
    }

    @Test
    public void testNegativeValidateNotEmpty() {
        assertThrows(MESSAGE_EXPECTED, ApollonException.class, () -> ValidationHelper.validateNotEmpty(Collections.emptyMap(), MESSAGE_TEMPLATE, 1));
    }

    @Test
    public void testPositiveValidateTrue() {
        ValidationHelper.validateTrue(true, MESSAGE_TEMPLATE, 1);
    }

    @Test
    public void testNegativeValidateTrue() {
        assertThrows(MESSAGE_EXPECTED, ApollonException.class, () -> ValidationHelper.validateTrue(false, MESSAGE_TEMPLATE, 1));
    }
}