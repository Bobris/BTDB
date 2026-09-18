using System;

namespace BTDB.Replication;

internal enum PublicationResolution { Pending, ExactResultObserved, Changed }

/// <summary>A consistent read of content, metadata and opaque CAS token from one blob version.</summary>
internal sealed class TrlSnapshot(string token, ReadOnlySpan<byte> content, TrlMetadata metadata)
{
    readonly byte[] _content = content.ToArray();
    public string Token { get; } = token;
    public ReadOnlySpan<byte> Content => _content;
    public TrlMetadata Metadata { get; } = metadata;
}

/// <summary>
/// Immutable CAS intent. Only the publication lane may dispatch it, after checking live authority. M2 must supply
/// validated complete native transaction cuts and M3 must verify successor readiness; these helpers do not parse TRL.
/// </summary>
internal sealed class TrlPublication
{
    readonly byte[] _content;
    TrlPublication(string? expectedToken, ReadOnlySpan<byte> content, TrlMetadata metadata)
    {
        ExpectedToken = expectedToken;
        _content = content.ToArray();
        Metadata = metadata;
    }

    public string? ExpectedToken { get; }
    public ReadOnlySpan<byte> Content => _content;
    public TrlMetadata Metadata { get; }

    public static TrlPublication Genesis(ReadOnlySpan<byte> completeNativeContent, ulong term, TrlSuccessor? next = null)
    {
        var metadata = new TrlMetadata(term, next);
        _ = metadata.Encode();
        return new(null, completeNativeContent, metadata);
    }

    public static TrlPublication Append(TrlSnapshot current, ulong term, ReadOnlySpan<byte> suffix,
        TrlSuccessor? preparedSuccessor = null)
    {
        if (current.Metadata.Next is not null || current.Metadata.Term != term)
            throw new InvalidOperationException("Only the owning term can append to an unlinked tail.");
        var metadata = new TrlMetadata(term, preparedSuccessor);
        _ = metadata.Encode();
        var content = new byte[checked(current.Content.Length + suffix.Length)];
        current.Content.CopyTo(content);
        suffix.CopyTo(content.AsSpan(current.Content.Length));
        return new(current.Token, content, metadata);
    }

    public static TrlPublication Adopt(TrlSnapshot current, ulong newTerm)
    {
        if (current.Metadata.Next is not null)
            throw new InvalidOperationException("Follow the selected continuation before adopting its writable tail.");
        if (newTerm <= current.Metadata.Term)
            throw new InvalidOperationException("Adoption requires a newer selected term.");
        return new(current.Token, current.Content, new(newTerm));
    }

    public PublicationResolution Reconcile(TrlSnapshot? current)
    {
        // Missing/old state is not a rejection: the outstanding request may still land later.
        if (current is null || current.Token == ExpectedToken) return PublicationResolution.Pending;
        return current.Metadata == Metadata && current.Content.SequenceEqual(_content)
            ? PublicationResolution.ExactResultObserved : PublicationResolution.Changed;
        // Changed means re-read/adopt actual history, NEVER blindly retry the old mutation or infer it never landed.
    }
}
