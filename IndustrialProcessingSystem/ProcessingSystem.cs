using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IndustrialProcessingSystem
{
    public class ProcessingSystem
    {
        private readonly List<Job> queue = new List<Job>();
        private readonly object locker = new object();

        private readonly int workerCount;
        private readonly int maxQueueSize;

        private readonly HashSet<Guid> submittedJobs = new HashSet<Guid>();
        private readonly Dictionary<Guid, Job> allJobs = new Dictionary<Guid, Job>();
        private readonly Dictionary<Guid, TaskCompletionSource<int>> results = new Dictionary<Guid, TaskCompletionSource<int>>();

        private readonly List<JobEventArgs> completed = new List<JobEventArgs>();
        private readonly List<JobEventArgs> failed = new List<JobEventArgs>();

        private int reportIndex = 0;

        public event EventHandler<JobEventArgs> JobCompleted;
        public event EventHandler<JobEventArgs> JobFailed;

        public ProcessingSystem(string xmlPath)
        {
            XElement xml = XElement.Load(xmlPath);

            workerCount = int.Parse(xml.Element("WorkerCount").Value);
            maxQueueSize = int.Parse(xml.Element("MaxQueueSize").Value);

            foreach(var jobXml in xml.Element("Jobs").Elements("Job"))
            {
                Job job = new Job
                {
                    Type = (JobType)Enum.Parse(typeof(JobType), jobXml.Attribute("Type").Value),
                    Payload = jobXml.Attribute("Payload").Value,
                    Priority = int.Parse(jobXml.Attribute("Priority").Value)
                };

                Submit(job);
            }

            StartWorkers();

            Task.Run(async () =>
            {
                while(true)
                {
                    await Task.Delay(60000);
                    GenerateReport();
                }
            });
        }

        public JobHandle Submit(Job job)
        {
            lock(locker)
            {
                if(submittedJobs.Contains(job.Id))
                    return new JobHandle { Id = job.Id, Result = results[job.Id].Task };

                if(queue.Count >= maxQueueSize)
                    throw new Exception("Queue is full!");

                submittedJobs.Add(job.Id);
                allJobs[job.Id] = job;

                var tcs = new TaskCompletionSource<int>();
                results[job.Id] = tcs;

                queue.Add(job);
                queue.Sort((a, b) => a.Priority.CompareTo(b.Priority));

                Monitor.Pulse(locker);

                return new JobHandle
                {
                    Id = job.Id,
                    Result = tcs.Task
                };
            }
        }

        private void StartWorkers()
        {
            for(int i = 0; i < workerCount; i++)
            {
                Task.Run(() => WorkerLoop());
            }
        }

        private void WorkerLoop()
        {
            while(true)
            {
                Job job;

                lock(locker)
                {
                    while(queue.Count == 0)
                    {
                        Monitor.Wait(locker);
                    }

                    job = queue[0];
                    queue.RemoveAt(0);
                }

                ProcessWithRetry(job);
            }
        }

        private void ProcessWithRetry(Job job)
        {
            int attempt = 0;

            while(attempt < 3)
            {
                attempt++;

                Stopwatch sw = Stopwatch.StartNew();

                try
                {
                    Task<int> task = Task.Run(() => ExecuteJob(job));

                    bool finishedInTime = task.Wait(2000);

                    sw.Stop();

                    if(!finishedInTime)
                    {
                        throw new TimeoutException("Job took longer than 2 seconds.");
                    }

                    int result = task.Result;

                    results[job.Id].TrySetResult(result);

                    var args = new JobEventArgs
                    {
                        JobId = job.Id,
                        Type = job.Type,
                        Result = result,
                        Status = "COMPLETED",
                        Duration = sw.Elapsed
                    };

                    lock(locker)
                    {
                        completed.Add(args);
                    }

                    JobCompleted?.Invoke(this, args);
                    return;
                }
                catch
                {
                    sw.Stop();

                    var args = new JobEventArgs
                    {
                        JobId = job.Id,
                        Type = job.Type,
                        Result = -1,
                        Status = attempt == 3 ? "ABORT" : "FAILED",
                        Duration = sw.Elapsed
                    };

                    lock(locker)
                    {
                        failed.Add(args);
                    }

                    JobFailed?.Invoke(this, args);

                    if(attempt == 3)
                    {
                        results[job.Id].TrySetResult(-1);
                        return;
                    }
                }
            }
        }

        private int ExecuteJob(Job job)
        {
            if(job.Type == JobType.Prime)
                return ExecutePrimeJob(job.Payload);

            if(job.Type == JobType.IO)
                return ExecuteIOJob(job.Payload);

            throw new Exception("Unknown job type.");
        }

        private int ExecutePrimeJob(string payload)
        {
            // numbers:10_000,threads:3

            string[] parts = payload.Split(',');

            int numbers = int.Parse(parts[0].Split(':')[1].Replace("_", ""));
            int threads = int.Parse(parts[1].Split(':')[1]);

            if(threads < 1) threads = 1;
            if(threads > 8) threads = 8;

            int count = 0;
            object countLocker = new object();

            Parallel.For(
                2,
                numbers + 1,
                new ParallelOptions { MaxDegreeOfParallelism = threads },
                number =>
                {
                    if(IsPrime(number))
                    {
                        lock(countLocker)
                        {
                            count++;
                        }
                    }
                });

            return count;
        }

        private bool IsPrime(int number)
        {
            if(number < 2) return false;

            for(int i = 2; i <= Math.Sqrt(number); i++)
            {
                if(number % i == 0)
                    return false;
            }

            return true;
        }

        private int ExecuteIOJob(string payload)
        {
            // delay:1_000

            int delay = int.Parse(payload.Split(':')[1].Replace("_", ""));

            Thread.Sleep(delay);

            Random random = new Random();
            return random.Next(0, 101);
        }

        public IEnumerable<Job> GetTopJobs(int n)
        {
            lock(locker)
            {
                return queue
                    .OrderBy(j => j.Priority)
                    .Take(n)
                    .ToList();
            }
        }

        public Job GetJob(Guid id)
        {
            lock(locker)
            {
                if(allJobs.ContainsKey(id))
                    return allJobs[id];

                return null;
            }
        }

        public void GenerateReport()
        {
            List<JobEventArgs> completedCopy;
            List<JobEventArgs> failedCopy;

            lock(locker)
            {
                completedCopy = completed.ToList();
                failedCopy = failed.ToList();
            }

            var completedByType =
                from job in completedCopy
                group job by job.Type into g
                select new
                {
                    Type = g.Key,
                    Count = g.Count(),
                    AverageTime = g.Average(x => x.Duration.TotalMilliseconds)
                };

            var failedByType =
                from job in failedCopy
                group job by job.Type into g
                orderby g.Key
                select new
                {
                    Type = g.Key,
                    Count = g.Count()
                };

            XElement report = new XElement("Report",
                new XElement("CompletedJobs",
                    completedByType.Select(x =>
                        new XElement("JobType",
                            new XAttribute("Type", x.Type),
                            new XAttribute("Count", x.Count),
                            new XAttribute("AverageTimeMs", x.AverageTime)
                        )
                    )
                ),
                new XElement("FailedJobs",
                    failedByType.Select(x =>
                        new XElement("JobType",
                            new XAttribute("Type", x.Type),
                            new XAttribute("Count", x.Count)
                        )
                    )
                )
            );

            string fileName = $"Report_{reportIndex % 10}.xml";
            report.Save(fileName);

            reportIndex++;
        }
    }
}