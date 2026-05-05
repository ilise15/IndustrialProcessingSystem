using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace IndustrialProcessingSystem
{
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                ProcessingSystem system = new ProcessingSystem("SystemConfig.xml");

                system.JobCompleted += async (sender, e) =>
                {
                    string text = $"[{DateTime.Now}] [{e.Status}] {e.JobId}, Result: {e.Result}";
                    await WriteLog(text);
                };

                system.JobFailed += async (sender, e) =>
                {
                    string text = $"[{DateTime.Now}] [{e.Status}] {e.JobId}, Result: {e.Result}";
                    await WriteLog(text);
                };

                Random random = new Random();

                XElement xml = XElement.Load("SystemConfig.xml");
                int producerCount = int.Parse(xml.Element("WorkerCount").Value);

                for(int i = 0; i < producerCount; i++)
                {
                    Task.Run(() =>
                    {
                        while(true)
                        {
                            try
                            {
                                Job job = CreateRandomJob(random);
                                system.Submit(job);

                                Thread.Sleep(random.Next(500, 1500));
                            }
                            catch(Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                            }
                        }
                    });
                }

                Console.WriteLine("Processing system started.");
                Console.WriteLine("Press any key to stop...");
                Console.ReadKey();

                system.GenerateReport();
            }
            catch(Exception ex)
            {
                Console.WriteLine("ERROR: " + ex.Message);
            }
        }

        static Job CreateRandomJob(Random random)
        {
            int type = random.Next(0, 2);

            if(type == 0)
            {
                return new Job
                {
                    Type = JobType.Prime,
                    Payload = $"numbers:{random.Next(5000, 20000)},threads:{random.Next(1, 9)}",
                    Priority = random.Next(1, 5)
                };
            }
            else
            {
                return new Job
                {
                    Type = JobType.IO,
                    Payload = $"delay:{random.Next(500, 4000)}",
                    Priority = random.Next(1, 4)
                };
            }
        }

        private static readonly object logLocker = new object();

        static Task WriteLog(string text)
        {
            lock(logLocker)
            {
                File.AppendAllText("log.txt", text + Environment.NewLine);
            }

            return Task.CompletedTask;
        }
    }
}