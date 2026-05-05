using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IndustrialProcessingSystem
{
    public class JobEventArgs :EventArgs
    {
        public Guid JobId { get; set; }
        public JobType Type { get; set; }
        public int Result { get; set; }
        public string Status { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
