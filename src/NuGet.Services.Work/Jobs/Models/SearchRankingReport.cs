using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NuGet.Services.Work.Jobs.Models
{
    public class SearchRankingReport
    {
        public IList<SearchRankingEntry> Overall { get; set; }
        public IDictionary<string, IList<SearchRankingEntry>> ByProjectType { get; set; }

        public SearchRankingReport()
        {
            Overall = new List<SearchRankingEntry>();
            ByProjectType = new Dictionary<string, IList<SearchRankingEntry>>();
        }
    }
}
