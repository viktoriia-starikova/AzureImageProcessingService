using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImageProcessingService
{
    internal class TaskState
    {
        [JsonProperty(PropertyName = "id")]
        public string TaskId { get; set; }

        public string FileName { get; set; }
        public string State { get; set; }
        public string OriginalFilePath { get; set; }
        public string ProcessedFilePath { get; set; }
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
