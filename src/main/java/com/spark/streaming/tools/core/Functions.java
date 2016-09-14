package com.spark.streaming.tools.core;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Utility {@link Function2} implementations.
 */
public final class Functions {

    private Functions() {}

    /**
     * @return a function that returns the second of two values
     * @param <T> element type
     */
    public static <T> Function2<T,T,T> last() {
        return new Function2<T,T,T>() {
            @Override
            public T call(T current, T next) {
                return next;
            }
        };
    }

    public static <T> VoidFunction<T> noOp() {
        return new VoidFunction<T>() {
            @Override
            public void call(T t) {
                // do nothing
            }
        };
    }

    public static <T> Function<T,T> identity() {
        return new Function<T,T>() {
            @Override
            public T call(T t) {
                return t;
            }
        };
    }

}