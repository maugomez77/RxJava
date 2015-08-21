/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rx.plugins;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;

/**
 * Abstract ExecutionHook with invocations at different lifecycle points of {@link Observable} execution with a
 * default no-op implementation.
 * <p>
 * See {@link RxJavaPlugins} or the RxJava GitHub Wiki for information on configuring plugins:
 * <a href="https://github.com/ReactiveX/RxJava/wiki/Plugins">https://github.com/ReactiveX/RxJava/wiki/Plugins</a>.
 * <p>
 * <b>Note on thread-safety and performance:</b>
 * <p>
 * A single implementation of this class will be used globally so methods on this class will be invoked
 * concurrently from multiple threads so all functionality must be thread-safe.
 * <p>
 * Methods are also invoked synchronously and will add to execution time of the observable so all behavior
 * should be fast. If anything time-consuming is to be done it should be spawned asynchronously onto separate
 * worker threads.
 * 
 */
public abstract class RxJavaObservableExecutionHook {
    /**
     * Invoked during the construction by {@link Observable#create(OnSubscribe)}
     *
     * This can be used to decorate or replace the <code>onSubscribe</code> function or just perform extra
     * logging, metrics and other such things and pass-thru the function.
     *
     * @param <T> t
     * @param f
     *            original to be executed
     * @return function that can be modified, decorated, replaced or just
     *         returned as a pass-thru
     */
    public <T> OnSubscribe<T> onCreate(OnSubscribe<T> f) {
        return f;
    }

    /**
     * Invoked before {@link Observable#subscribe(rx.Subscriber)} is about to be executed.
     *
     * This can be used to decorate or replace the <code>onSubscribe</code> function or just perform extra
     * logging, metrics and other such things and pass-thru the function.
     *
     * @param observableInstance  p
     * @param <T> t
     * @param onSubscribe
     *            original to be executed
     * @return function that can be modified, decorated, replaced or just
     *         returned as a pass-thru
     */
    public <T> OnSubscribe<T> onSubscribeStart(Observable<? extends T> observableInstance, final OnSubscribe<T> onSubscribe) {
        // pass-thru by default
        return onSubscribe;
    }

    /**
     * Invoked after successful execution of  with returned
     *
     * This can be used to decorate or replace the {@link Subscription} instance or just perform extra logging,
     * metrics and other such things and pass-thru the subscription.
     *
     * @param <T> t
     * @param subscription
     *            original {@link Subscription}
     * @return {@link Subscription} subscription that can be modified, decorated, replaced or just returned as a
     *         pass-thru
     */
    public <T> Subscription onSubscribeReturn(Subscription subscription) {
        // pass-thru by default
        return subscription;
    }

    /**
     * Invoked after failed execution of with thrown Throwable.
     *
     * @param <T> t
     * @param e
     *            Throwable thrown by
     * @return Throwable that can be decorated, replaced or just returned as a pass-thru
     */
    public <T> Throwable onSubscribeError(Throwable e) {
        // pass-thru by default
        return e;
    }

    /**
     * Invoked just as the operator functions is called to bind two operations together into a new
     *  and the return value is used as the lifted function
     *
     * This can be used to decorate or replace the instance or just perform extra
     * logging, metrics and other such things and pass-thru the onSubscribe.
     *
     * @param <T> t
     * @param <R> r
     * @param lift
     *            original
     * @return function that can be modified, decorated, replaced or just
     *         returned as a pass-thru
     */
    public <T, R> Operator<? extends R, ? super T> onLift(final Operator<? extends R, ? super T> lift) {
        return lift;
    }
}
