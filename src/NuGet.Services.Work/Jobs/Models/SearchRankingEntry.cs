using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs.Models
{
    public class SearchRankingEntry
    {
        public string PackageId { get; set; }
        public int Downloads { get; set; }
    }
}
