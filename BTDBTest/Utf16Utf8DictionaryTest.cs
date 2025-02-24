using System;
using System.Collections.Generic;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

public class Utf16Utf8DictionaryTest
{
    [Fact]
    public void ContainsKey_Utf16KeyExists_ReturnsTrue()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        Assert.True(dictionary.ContainsKey("key1".AsSpan()));
    }

    [Fact]
    public void ContainsKey_Utf16KeyDoesNotExist_ReturnsFalse()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        Assert.False(dictionary.ContainsKey("key3".AsSpan()));
    }

    [Fact]
    public void ContainsKey_Utf8KeyExists_ReturnsTrue()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var key = "key1"u8.ToArray();
        Assert.True(dictionary.ContainsKey(key.AsSpan()));
    }

    [Fact]
    public void ContainsKey_Utf8KeyDoesNotExist_ReturnsFalse()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var key = "key3"u8.ToArray();
        Assert.False(dictionary.ContainsKey(key.AsSpan()));
    }

    [Fact]
    public void TryGetValue_Utf16KeyExists_ReturnsTrueAndValue()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        dictionary.GetValueRef("key1".AsSpan(), out _) = 42;
        Assert.True(dictionary.TryGetValue("key1".AsSpan(), out var value));
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGetValue_Utf16KeyDoesNotExist_ReturnsFalse()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        Assert.False(dictionary.TryGetValue("key3".AsSpan(), out var value));
        Assert.Equal(0, value);
    }

    [Fact]
    public void TryGetValue_Utf8KeyExists_ReturnsTrueAndValue()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        dictionary.GetValueRef("key1".AsSpan(), out _) = 42;
        var key = "key1"u8.ToArray();
        Assert.True(dictionary.TryGetValue(key.AsSpan(), out var value));
        Assert.Equal(42, value);
    }

    [Fact]
    public void TryGetValue_Utf8KeyDoesNotExist_ReturnsFalse()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var key = "key3"u8.ToArray();
        Assert.False(dictionary.TryGetValue(key.AsSpan(), out var value));
        Assert.Equal(0, value);
    }

    [Fact]
    public void GetIndex_Utf16KeyExists_ReturnsIndex()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var index = dictionary.GetIndex("key1".AsSpan());
        Assert.NotEqual(-1, index);
    }

    [Fact]
    public void GetIndex_Utf16KeyDoesNotExist_ReturnsMinusOne()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var index = dictionary.GetIndex("key3".AsSpan());
        Assert.Equal(-1, index);
    }

    [Fact]
    public void GetIndex_Utf8KeyExists_ReturnsIndex()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var key = "key1"u8.ToArray();
        var index = dictionary.GetIndex(key.AsSpan());
        Assert.NotEqual(-1, index);
    }

    [Fact]
    public void GetIndex_Utf8KeyDoesNotExist_ReturnsMinusOne()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        var key = "key3"u8.ToArray();
        var index = dictionary.GetIndex(key.AsSpan());
        Assert.Equal(-1, index);
    }

    [Fact]
    public void ValueRef_Utf16Key_ReturnsReference()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        dictionary.GetValueRef("key1".AsSpan(), out var found) = 42;
        Assert.True(found);
        ref var value = ref dictionary.ValueRef((uint)dictionary.GetIndex("key1".AsSpan()));
        Assert.Equal(42, value);
    }

    [Fact]
    public void ValueRef_Utf8Key_ReturnsReference()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        dictionary.GetValueRef("key1".AsSpan(), out var found) = 42;
        Assert.True(found);
        var key = "key1"u8.ToArray();
        ref var value = ref dictionary.ValueRef((uint)dictionary.GetIndex(key.AsSpan()));
        Assert.Equal(42, value);
    }

    [Fact]
    public void ValueRef_DerivedUtf16Utf8Dictionary_ReturnsReference()
    {
        var parent = new Utf16Utf8Dictionary<uint>(2, ["key1", "key2"]);
        var dictionary = new DerivedUtf16Utf8Dictionary<int, uint>(parent);
        dictionary.GetValueRef("key1", out var found) = 42;
        Assert.True(found);
        ref var value = ref dictionary.ValueRef((uint)parent.GetIndex("key1"));
        Assert.Equal(42, value);
    }

    [Fact]
    public void ValueRef_DerivedUtf16Utf8Dictionary_Utf8Key_ReturnsReference()
    {
        var parent = new Utf16Utf8Dictionary<uint>(2, ["key1", "key2"]);
        var dictionary = new DerivedUtf16Utf8Dictionary<int, uint>(parent);
        dictionary.GetValueRef("key1"u8, out var found) = 42;
        Assert.True(found);
        ref var value = ref dictionary.ValueRef((uint)parent.GetIndex("key1"u8));
        Assert.Equal(42, value);
    }

    [Fact]
    public void CanIterateOverDictionaryWhilePreservingInsertOrder()
    {
        var dictionary = new Utf16Utf8Dictionary<int>(2, ["key1", "key2"]);
        dictionary.GetValueRef("key1", out _) = 42;
        dictionary.GetValueRef("key2", out _) = 43;
        var keys = new List<string>();
        var ss = 0;
        foreach (var i in dictionary.Index)
        {
            keys.Add(new(dictionary.KeyUtf16(i)));
            ss = ss * 100 + dictionary.ValueRef(i);
        }

        Assert.Equal(new[] { "key1", "key2" }, keys);
        Assert.Equal(4243, ss);
    }

    [SkipWhen(SkipWhenAttribute.Is.Release, "Duplicate keys are checkec only in debug mode.")]
    public void InDebugConstructorThrowsOnDuplicateKeys()
    {
        Assert.Throws<ArgumentException>(() => new Utf16Utf8Dictionary<int>(2, ["key1", "key1"]));
    }
}
