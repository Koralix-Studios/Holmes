package com.koralix.holmes.cache;

import java.util.function.Function;

public interface Cache<K, V> {

    V get(K key);

    void put(K key, V value);

    V computeIfAbsent(K key, Function<K, V> function);

    void refresh(K key);

    void clear();
}
