using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using IndustrialProcessingSystem;

namespace IndustrialProcessingSystem.Tests
{
    [TestClass]
    public class ProcessingSystemTests
    {
        private string CreateConfig(int workerCount, int maxQueueSize)
        {
            string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".xml");

            string xml = $@"<?xml version=""1.0"" encoding=""utf-8""?>
<SystemConfig>
    <WorkerCount>{workerCount}</WorkerCount>
    <MaxQueueSize>{maxQueueSize}</MaxQueueSize>
    <Jobs>
    </Jobs>
</SystemConfig>";

            File.WriteAllText(path, xml);
            return path;
        }

        [TestMethod]
        public void Submit_ShouldStoreJob()
        {
            string config = CreateConfig(0, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job = new Job
            {
                Type = JobType.Prime,
                Payload = "numbers:10,threads:1",
                Priority = 1
            };

            system.Submit(job);

            Job found = system.GetJob(job.Id);

            Assert.IsNotNull(found);
            Assert.AreEqual(job.Id, found.Id);
            Assert.AreEqual(JobType.Prime, found.Type);
        }

        [TestMethod]
        public void GetTopJobs_ShouldReturnJobsByPriority()
        {
            string config = CreateConfig(0, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job low = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 3
            };

            Job high = new Job
            {
                Type = JobType.Prime,
                Payload = "numbers:10,threads:1",
                Priority = 1
            };

            Job medium = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 2
            };

            system.Submit(low);
            system.Submit(high);
            system.Submit(medium);

            var topJobs = system.GetTopJobs(2).ToList();

            Assert.AreEqual(2, topJobs.Count);
            Assert.AreEqual(high.Id, topJobs[0].Id);
            Assert.AreEqual(medium.Id, topJobs[1].Id);
        }

        [TestMethod]
        public void Submit_ShouldRejectJob_WhenQueueIsFull()
        {
            string config = CreateConfig(0, 1);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job1 = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 1
            };

            Job job2 = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 1
            };

            system.Submit(job1);

            Assert.ThrowsException<Exception>(() =>
            {
                system.Submit(job2);
            });
        }

        [TestMethod]
        public void Submit_ShouldBeIdempotent_ForSameJobId()
        {
            string config = CreateConfig(0, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Guid id = Guid.NewGuid();

            Job job = new Job
            {
                Id = id,
                Type = JobType.Prime,
                Payload = "numbers:10,threads:1",
                Priority = 1
            };

            JobHandle firstHandle = system.Submit(job);
            JobHandle secondHandle = system.Submit(job);

            Assert.AreEqual(firstHandle.Id, secondHandle.Id);
            Assert.AreSame(firstHandle.Result, secondHandle.Result);
        }

        [TestMethod]
        public async Task PrimeJob_ShouldReturnCorrectPrimeCount()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job = new Job
            {
                Type = JobType.Prime,
                Payload = "numbers:10,threads:1",
                Priority = 1
            };

            JobHandle handle = system.Submit(job);

            Task finished = await Task.WhenAny(handle.Result, Task.Delay(5000));

            Assert.AreSame(handle.Result, finished);

            int result = await handle.Result;

            Assert.AreEqual(4, result);
        }

        [TestMethod]
        public async Task IOJob_ShouldReturnNumberBetween0And100()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 1
            };

            JobHandle handle = system.Submit(job);

            Task finished = await Task.WhenAny(handle.Result, Task.Delay(5000));

            Assert.AreSame(handle.Result, finished);

            int result = await handle.Result;

            Assert.IsTrue(result >= 0 && result <= 100);
        }

        [TestMethod]
        public async Task LongIOJob_ShouldFailAndReturnMinusOne()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job = new Job
            {
                Type = JobType.IO,
                Payload = "delay:3000",
                Priority = 1
            };

            JobHandle handle = system.Submit(job);

            Task finished = await Task.WhenAny(handle.Result, Task.Delay(10000));

            Assert.AreSame(handle.Result, finished);

            int result = await handle.Result;

            Assert.AreEqual(-1, result);
        }


        [TestMethod]
        public void GetJob_ShouldReturnNull_WhenJobDoesNotExist()
        {
            string config = CreateConfig(0, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            var result = system.GetJob(Guid.NewGuid());

            Assert.IsNull(result);
        }

        [TestMethod]
        public void GetTopJobs_ShouldReturnEmpty_WhenQueueIsEmpty()
        {
            string config = CreateConfig(0, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            var result = system.GetTopJobs(5);

            Assert.AreEqual(0, result.Count());
        }

        [TestMethod]
        public async Task IOJob_ShortDelay_ShouldComplete()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            Job job = new Job
            {
                Type = JobType.IO,
                Payload = "delay:100",
                Priority = 1
            };

            var handle = system.Submit(job);
            int result = await handle.Result;

            Assert.IsTrue(result >= 0);
        }
        [TestMethod]
        public async Task JobCompleted_Event_ShouldBeTriggered()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            bool eventTriggered = false;

            system.JobCompleted += (s, e) =>
            {
                eventTriggered = true;
            };

            Job job = new Job
            {
                Type = JobType.Prime,
                Payload = "numbers:10,threads:1",
                Priority = 1
            };

            var handle = system.Submit(job);

            await handle.Result;

            await Task.Delay(500);

            Assert.IsTrue(eventTriggered);
        }

        [TestMethod]
        public async Task JobFailed_Event_ShouldBeTriggered()
        {
            string config = CreateConfig(1, 10);
            ProcessingSystem system = new ProcessingSystem(config);

            bool failedTriggered = false;

            system.JobFailed += (s, e) =>
            {
                failedTriggered = true;
            };

            Job job = new Job
            {
                Type = JobType.IO,
                Payload = "delay:3000", // > 2s → fail
                Priority = 1
            };

            var handle = system.Submit(job);

            await handle.Result;

            Assert.IsTrue(failedTriggered);
        }
    }
}