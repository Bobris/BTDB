using System;
using System.Collections.Generic;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDBTest;

/// Shared setup for field handler / ODB coverage tests: in-memory BTree KV DB, ObjectDB with auto registration,
/// symmetric cipher for EncryptedString and logger capturing type incompatibility reports.
public abstract class FieldHandlerCoverageTestBase : IDisposable
{
    protected IKeyValueDB _lowDb;
    protected IObjectDB _db;
    protected readonly List<string> LoggedMessages = new();

    protected FieldHandlerCoverageTestBase()
    {
        _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            CompactorScheduler = null,
            Compression = new NoCompressionStrategy(),
            FileCollection = new InMemoryFileCollection()
        });
        _db = OpenDb();
    }

    protected virtual DBOptions CreateOptions() => new DBOptions()
        .WithFieldHandlerLogger(new DefaultFieldHandlerLogger(s => LoggedMessages.Add(s)))
        .WithSymmetricCipher(new AesGcmSymmetricCipher(new byte[]
        {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
            27, 28, 29, 30, 31
        }));

    IObjectDB OpenDb(DBOptions? options = null)
    {
        var db = new ObjectDB();
        db.Open(_lowDb, false, options ?? CreateOptions());
        return db;
    }

    protected void ReopenDb(DBOptions? options = null)
    {
        _db.Dispose();
        _db = OpenDb(options);
    }

    /// Switches all IFieldHandler.UseNoEmit* flags (emit == true means IL generated handlers) and reopens DB.
    /// Must be used only from tests in non parallel collection "IFieldHandler.UseNoEmitForRelations".
    protected IDisposable EmitMode(bool emit)
    {
        var scope = new EmitScope(emit);
        ReopenDb();
        return scope;
    }

    sealed class EmitScope : IDisposable
    {
        readonly bool _oldNoEmit;
        readonly bool _oldNoEmitForKeyValue;
        readonly bool _oldNoEmitForRelations;

        public EmitScope(bool emit)
        {
            _oldNoEmit = IFieldHandler.UseNoEmit;
            _oldNoEmitForKeyValue = IFieldHandler.UseNoEmitForKeyValue;
            _oldNoEmitForRelations = IFieldHandler.UseNoEmitForRelations;
            IFieldHandler.UseNoEmit = !emit;
            IFieldHandler.UseNoEmitForKeyValue = !emit;
            IFieldHandler.UseNoEmitForRelations = !emit;
            ObjectDB.ResetAllMetadataCaches();
        }

        public void Dispose()
        {
            IFieldHandler.UseNoEmit = _oldNoEmit;
            IFieldHandler.UseNoEmitForKeyValue = _oldNoEmitForKeyValue;
            IFieldHandler.UseNoEmitForRelations = _oldNoEmitForRelations;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    public virtual void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }

    /// Stores singleton-like root object of type TOld under given name and then loads it back as TNew
    /// (registered under the same name) after reopening the database.
    protected TNew StoreAndReloadAs<TOld, TNew>(TOld value, string name) where TOld : class where TNew : class
    {
        _db.RegisterType(typeof(TOld), name);
        using (var tr = _db.StartTransaction())
        {
            tr.Store(value);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(TNew), name);
        using (var tr = _db.StartReadOnlyTransaction())
        {
            foreach (var item in tr.Enumerate<TNew>())
                return item;
            throw new InvalidOperationException("Nothing stored");
        }
    }
}
