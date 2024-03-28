/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.farao_community.farao.core_cc_post_processing.app.exception;

import org.springframework.core.NestedExceptionUtils;

/**
 * Custom abstract exception to be extended by all application exceptions.
 * Any subclass may be automatically wrapped to a JSON API error message if needed
 *
 * @author Ameni Walha {@literal <ameni.walha at rte-france.com>}
 */
public abstract class AbstractCoreCCPostProcessingException extends RuntimeException {

    protected AbstractCoreCCPostProcessingException(String message) {
        super(message);
    }

    protected AbstractCoreCCPostProcessingException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public abstract int getStatus();

    public abstract String getCode();

    public final String getTitle() {
        return getMessage();
    }

    public final String getDetails() {
        return NestedExceptionUtils.buildMessage(getMessage(), getCause());
    }
}
