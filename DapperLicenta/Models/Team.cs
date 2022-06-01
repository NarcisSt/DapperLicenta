using System;
using System.Collections.Generic;

namespace DapperLicenta.Models
{
    public partial class Team
    {
        public Team()
        {
            PlayerNameNavigations = new HashSet<Player>();
            PlayerTeamNavigations = new HashSet<Player>();
        }

        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public string Country { get; set; } = null!;
        public string City { get; set; } = null!;
        public long? Points { get; set; }

        public virtual ICollection<Player> PlayerNameNavigations { get; set; }
        public virtual ICollection<Player> PlayerTeamNavigations { get; set; }
    }
}
