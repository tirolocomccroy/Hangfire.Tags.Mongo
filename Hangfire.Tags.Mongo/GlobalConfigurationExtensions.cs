using Hangfire.Dashboard;
using Hangfire.Mongo;

namespace Hangfire.Tags.Mongo
{
    /// <summary>
    /// Provides extension methods to setup Hangfire.Tags
    /// </summary>
    public static class GlobalConfigurationExtensions
    {
        /// <summary>
        /// Configures Hangfire to use Tags.
        /// </summary>
        /// <param name="configuration">Global configuration</param>
        /// <param name="options">Options for tags</param>
        /// <param name="mongoOptions">Options for Mongo storage</param>
        /// <returns></returns>
        public static IGlobalConfiguration UseTagsWithMongo(this IGlobalConfiguration configuration, TagsOptions options = null, MongoStorageOptions mongoOptions = null)
        {
            options = options ?? new TagsOptions();
            mongoOptions = mongoOptions ?? new MongoStorageOptions();

            options.Storage = new MongoTagsServiceStorage(mongoOptions);
            var config = configuration.UseTags(options);

            JobsSidebarMenu.Items.RemoveAll(x => x.Method.Module.Assembly == typeof(TagsOptions).Assembly);
            JobsSidebarMenu.Items.Add(page => new MenuItem("Tags", page.Url.To("/tags/search"))
            {
                Active = page.RequestPath.StartsWith("/tags/search"),
                Metric = new DashboardMetric("tags:count", razorPage =>
                {
                    return new Metric(((MongoTagsServiceStorage)options.Storage).GetTagsCount());
                })
            });

            return config;
        }
    }
}
