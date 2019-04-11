using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.Mongo;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Tags.Dashboard.Monitoring;
using Hangfire.Tags.Storage;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Tags.Mongo
{
    public class MongoTagsServiceStorage : ITagsServiceStorage
    {
        private readonly MongoStorageOptions _options;

        private MongoTagsMonitoringApi MonitoringApi => new MongoTagsMonitoringApi(JobStorage.Current.GetMonitoringApi());

        public MongoTagsServiceStorage()
            : this(new MongoStorageOptions())
        {
        }

        public MongoTagsServiceStorage(MongoStorageOptions options)
        {
            _options = options;
        }

        public ITagsTransaction GetTransaction(IWriteOnlyTransaction transaction)
        {
            return new MongoTagsTransaction(_options, transaction);
        }

        public long GetTagsCount()
        {
            MongoTagsMonitoringApi monitoringApi = MonitoringApi;
            return monitoringApi.DbContext.JobGraph
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Regex(x => x.Key, new BsonRegularExpression($"tags<.*")))
                .CountDocuments();
        }

        private Dictionary<string, List<SetDto>> GetTags(string setKey, string tagName)
        {
            MongoTagsMonitoringApi monitoringApi = MonitoringApi;

            Dictionary<string, List<SetDto>> tto = monitoringApi.DbContext.JobGraph
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Regex(x => x.Key, new BsonRegularExpression($"{setKey}:.*<{tagName.Replace("tags:", string.Empty)}>")))
                .ToList()
                .GroupBy(x => x.Value)
                .Select(x => new
                {
                    TagName = x.Key,
                    SetList = x.ToList()
                })
                .ToDictionary(k => k.TagName, k => k.SetList);

            return tto;
        }

        public IEnumerable<TagDto> SearchWeightedTags(string tag, string setKey)
        {
            MongoTagsMonitoringApi monitoringApi = MonitoringApi;
            return monitoringApi.DbContext.JobGraph
                .OfType<SetDto>()
                .Find(Builders<SetDto>.Filter.Regex(x => x.Key, new BsonRegularExpression($"tags<.*")))
                .ToList()
                .Select(x => new TagDto
                {
                    Amount = 1,
                    Tag = x.Value,
                    Percentage = 5
                });

            //Dictionary<string, List<SetDto>> tto = GetTags(setKey);
            //double ttot = tto.Sum(k => k.Value.Count);

            //IEnumerable<TagDto> res = tto.Select(k => new TagDto
            //{
            //    Amount = k.Value.Count,
            //    Tag = k.Key,
            //    Percentage = Convert.ToInt32(Math.Round(k.Value.Count / ttot * 100))
            //});

            //return res;
        }

        public IEnumerable<string> SearchTags(string tag, string setKey)
        {
            return GetTags(setKey, tag).SelectMany(k => k.Value.Select(w => w.Value));
        }

        public int GetJobCount(string[] tags, string stateName)
        {
            if (string.IsNullOrEmpty(stateName))
            {
                return GetJobStateCount(tags, Int32.MaxValue, 0).Sum(x => x.Value);
            }
            else
            {
                return GetJobStateCount(tags, Int32.MaxValue, 0)[stateName];
            }
        }

        private List<JobDto> GetJobs(string[] tags, int maxTags, int from)
        {
            MongoTagsMonitoringApi monitoringApi = MonitoringApi;
            Dictionary<string, List<SetDto>> tto = GetTags("tags", tags.FirstOrDefault());

            List<ObjectId> values = tags.SelectMany(x => tto[x.Replace("tags:", string.Empty)]).Select(w => new ObjectId(w.Key.Replace("tags:", string.Empty).Replace($"<{w.Value}>", string.Empty))).ToList();

            List<JobDto> jobs = monitoringApi.DbContext
                .JobGraph
                .OfType<JobDto>()
                .Find(Builders<JobDto>.Filter.In(x => x.Id, values))
                .Skip(from)
                .Limit(maxTags)
                .ToList();

            return jobs;
        }

        public IDictionary<string, int> GetJobStateCount(string[] tags, int maxTags = 50, int from = 0)
        {
            List<JobDto> jobs = GetJobs(tags, maxTags, from);

            Dictionary<string, int> group = jobs.GroupBy(x => x.StateName).Select(x => new
            {
                x.First().StateName,
                Count = x.Count()
            }).ToDictionary(x => x.StateName, x => x.Count);

            return group;
        }

        public JobList<MatchingJobDto> GetMatchingJobs(string[] tags, int from, int count, string stateName)
        {
            return GetJobs(from, count, tags, stateName,
                (sqlJob, job, stateData) =>
                    new MatchingJobDto
                    {
                        Job = job,
                        State = sqlJob.StateName,
                    });
        }

        private JobList<TDto> GetJobs<TDto>(int from, int count, string[] tags, string stateName,
            Func<JobSummary, Job, SafeDictionary<string, string>, TDto> selector)
        {
            List<JobDto> jobs = GetJobs(tags, Int32.MaxValue, from);

            List<JobSummary> joinedJobs = jobs
                .Where(job => string.IsNullOrEmpty(stateName) || job.StateName == stateName)
                .Skip(from)
                .Take(count)
                .Select(job =>
                {
                    var state = job.StateHistory.FirstOrDefault(s => s.Name == stateName);

                    return new JobSummary
                    {
                        Id = job.Id.ToString(),
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            return DeserializeJobs(joinedJobs, selector);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            InvocationData data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            ICollection<JobSummary> jobs,
            Func<JobSummary, Job, SafeDictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                var dto = default(TDto);

                if (job.InvocationData != null)
                {
                    //var deserializedData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                    var stateData = job.StateData != null
                        ? new SafeDictionary<string, string>(job.StateData, StringComparer.OrdinalIgnoreCase)
                        : null;

                    dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
                }

                result.Add(new KeyValuePair<string, TDto>(job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        public IDictionary<string, int> GetJobStateCount(string[] tags, int maxTags)
        {
            return GetJobStateCount(tags, maxTags, 0);
        }

        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/HangfireIO/Hangfire/issues/871
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get => ContainsKey(i) ? base[i] : default(TValue);
                set => base[i] = value;
            }
        }
    }
}
