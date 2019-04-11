using System;
using System.Data.Common;
using System.Reflection;
using Hangfire.Mongo;
using Hangfire.Mongo.Database;
using Hangfire.Storage;

namespace Hangfire.Tags.Mongo
{
    public class MongoTagsMonitoringApi
    {
        private readonly IMonitoringApi _monitoringApi;

        public HangfireDbContext DbContext { get; set; }

        public MongoTagsMonitoringApi(IMonitoringApi monitoringApi)
        {
            if (!(monitoringApi is MongoMonitoringApi))
                throw new ArgumentException("The monitor API is not implemented using Mongo", nameof(monitoringApi));
            _monitoringApi = monitoringApi;

            // Dirty, but lazy...we would like to execute these commands in the same transaction, so we're resorting to reflection for now


            if (DbContext == null)
            {
                DbContext = (HangfireDbContext)typeof(MongoMonitoringApi).GetField("_dbContext", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(monitoringApi);
            }
        }
    }
}
