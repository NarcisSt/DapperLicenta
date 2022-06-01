using System;
using System.Collections.Generic;

namespace DapperLicenta.Models
{
    public partial class Player
    {
        public long Id { get; set; }
        public string FirstName { get; set; } = null!;
        public string LastName { get; set; } = null!;
        public long ShirtNumber { get; set; }
        public string Position { get; set; } = null!;
        public long Age { get; set; }
        public string Team { get; set; } = null!;
        public int? Name { get; set; }

        public virtual Team? NameNavigation { get; set; }
        public virtual Team TeamNavigation { get; set; } = null!;
    }
}
