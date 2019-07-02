package org.elasticsearch.vpack;

import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;
import org.elasticsearch.vpack.tenant.rest.request.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.rest.RestRequest.Method.*;

/**
 * 抽象类，方便更多的地方植入rest级拦截。
 *
 * @param <T>
 */
public abstract class RestHandlerDispacher<T> {

    public RestHandlerDispacher() {
        register();
    }

    protected abstract void register();

    private final PathTrie<MethodHandlers> handlers = new PathTrie<>(RestUtils.REST_DECODER);

    public void registerHandler(RestRequest.Method method, String path, T handler) {
        handlers.insertOrUpdate(path, new MethodHandlers(path, handler, method), (mHandlers, newMHandler) -> mHandlers.addMethods(handler, method));
    }

    public Optional<T> getHandler(final RestRequest request) {
        final Map<String, String> originalParams = new HashMap<>(request.params());
        Iterator<MethodHandlers> allHandlers = handlers.retrieveAll(request.rawPath(), () -> request.params());
        for (Iterator<MethodHandlers> it = allHandlers; it.hasNext(); ) {
            Optional<T> mHandler = Optional.ofNullable(it.next()).flatMap(mh -> mh.getHandler(request.method()));
            if (mHandler.isPresent()) {
                request.params().clear();
                request.params().putAll(originalParams);
                return mHandler;
            }
        }
        request.params().clear();
        request.params().putAll(originalParams);
        return Optional.empty();
    }

    public final class MethodHandlers {
        private final String path;
        private final Map<RestRequest.Method, T> methodHandlers;

        MethodHandlers(String path, T handler, RestRequest.Method... methods) {
            this.path = path;
            this.methodHandlers = new HashMap<>(methods.length);
            for (RestRequest.Method method : methods) {
                methodHandlers.put(method, handler);
            }
        }

        public Optional<T> getHandler(RestRequest.Method method) {
            return Optional.ofNullable(methodHandlers.get(method));
        }

        public MethodHandlers addMethod(RestRequest.Method method, T handler) {
            T existing = methodHandlers.putIfAbsent(method, handler);
            if (existing != null) {
                throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
            }
            return this;
        }

        public MethodHandlers addMethods(T handler, RestRequest.Method... methods) {
            for (RestRequest.Method method : methods) {
                addMethod(method, handler);
            }
            return this;
        }

    }
}
