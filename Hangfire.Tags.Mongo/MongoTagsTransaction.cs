using System;
using System.Data.SqlClient;
using System.Reflection;
using Hangfire.Mongo;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using Hangfire.Tags.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Tags.Mongo
{
    internal class MongoTagsTransaction : ITagsTransaction
    {
        private readonly MongoStorageOptions _options;
        private readonly JobStorageTransaction _transaction;

        public HangfireDbContext DbContext { get; set; }

        public MongoTagsTransaction(MongoStorageOptions options, IWriteOnlyTransaction transaction)
        {
            if (!(transaction is MongoWriteOnlyTransaction))
                throw new ArgumentException("The transaction is not a Mongo transaction", nameof(transaction));

            _options = options;
            _transaction = (JobStorageTransaction)transaction;

            // Dirty, but lazy...we would like to execute these commands in the same transaction, so we're resorting to reflection for now

            DbContext = (HangfireDbContext)typeof(MongoWriteOnlyTransaction).GetTypeInfo().GetField("_dbContext", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(transaction);
        }

        public void ExpireSetValue(string key, string value, TimeSpan expireIn)
        {
            _transaction.ExpireSet(key, expireIn);
            _transaction.ExpireHash(key, expireIn);

            //DbContext.JobGraph
            //    .OfType<SetDto>()
            //    .UpdateMany(Builders<SetDto>.Filter.Eq(x => x.Key, key) & Builders<SetDto>.Filter.Eq(x => x.Value, value),
            //            Builders<SetDto>.Update.Set(k => k.ExpireAt, DateTime.UtcNow.Add(expireIn)));
        }

        public void PersistSetValue(string key, string value)
        {
            _transaction.PersistSet(key);
            _transaction.PersistHash(key);
            //_transaction.PersistHash(consoleId.GetHashKey());

            //DbContext.JobGraph
            //    .OfType<SetDto>()
            //    .UpdateMany(Builders<SetDto>.Filter.Eq(x => x.Key, key) & Builders<SetDto>.Filter.Eq(x => x.Value, value),
            //            Builders<SetDto>.Update.Set(k => k.ExpireAt, null));
        }
    }
}
