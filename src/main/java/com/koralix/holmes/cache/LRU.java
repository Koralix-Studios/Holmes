package com.koralix.holmes.cache;

import com.koralix.holmes.Holmes;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class LRU<K, V> implements Cache<K, V> {

    private final Map<K, V> map = new HashMap<>();
    private final Queue<K> queue = new LinkedList<>();
    private final int capacity;
    private final BiConsumer<K, V> save;

    public LRU(int capacity, BiConsumer<K, V> save) {
        this.capacity = capacity;
        this.save = save;
    }

    public void save() {
        map.forEach(save);
    }

    @Override
    public V get(K key) {
        refresh(key);
        return map.get(key);
    }

    @Override
    public void put(K key, V value) {
        map.put(key, value);
        refresh(key);
        if (map.size() > capacity) {
            K removedKey = queue.poll();
            save.accept(removedKey, map.remove(removedKey));
        }
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> function) {
        refresh(key);
        return map.computeIfAbsent(key, function);
    }

    @Override
    public void refresh(K key) {
        queue.remove(key);
        queue.add(key);
    }

    @Override
    public void clear() {
        save();
        map.clear();
        queue.clear();
    }
}
