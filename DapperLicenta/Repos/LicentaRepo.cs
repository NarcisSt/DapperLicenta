using Dapper;
using DapperLicenta.Datas;
using DapperLicenta.Models;
using Npgsql;
using StackExchange.Profiling;
using System.Data;
using System.Data.SqlClient;

namespace DapperLicenta.Repos
{

    public class LicentaRepo : ILicentaRepo
    {
        public static IDbConnection OpenConnection()
        {
            var conn = new NpgsqlConnection("Host=127.0.0.1;Port=5432;Database=postgres;Username=postgres;Password=admin");
            conn.Open();
            return new StackExchange.Profiling.Data.ProfiledDbConnection(conn, MiniProfiler.Current);
        }

        public void InsertTeam(TeamData teamData)
        {
            var mp = MiniProfiler.StartNew("InsertTeam");

            Team team = new Team()
            {
                City = teamData.City,
                Name = teamData.Name,
                Country = teamData.Country,
                Points = teamData.Points
            };

            using (var conn = OpenConnection())
            {
                var querySQL = @"INSERT INTO licenta.teams (name, country, city, points) VALUES (@name, @country, @city, @points);";
                using (mp.Step("Execute query"))
                {
                    conn.Execute(querySQL, teamData);
                }
                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
            }

        }

        public void UpdateTeam(TeamData teamData)
        {
            var mp = MiniProfiler.StartNew("UpdateTeam");

            using (var conn = OpenConnection())
            {
                var sqlUpdate = "UPDATE licenta.teams SET points = @points WHERE name = @name";

                using (mp.Step("Execute query"))
                {
                    conn.Execute(sqlUpdate, new { teamData.Points, teamData.Name });
                }
                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
            }

        }

        public void DeletePlayer(string position, long age)
        {
            var mp = MiniProfiler.StartNew("DeletePlayer");

            using (var conn = OpenConnection())
            {

                var sqlDelete = "DELETE FROM licenta.players WHERE position = @position AND age > @age";

                using (mp.Step("Execute query"))
                {
                    conn.Execute(sqlDelete, new { position, age });
                }
                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
            }
        }

        public int GetPlayerAndTeamByPosition(string position)
        {
            int players;

            var mp = MiniProfiler.StartNew("GetPlayerAndTeamByPosition");

            using (var conn = OpenConnection())
            {
                var sqlDelete = @"SELECT players.*, teams.country, teams.city, teams.points FROM licenta.players INNER JOIN licenta.teams ON players.team = teams.name
                       WHERE players.position = @position AND players.team LIKE '%United'";

                using (mp.Step("Execute query"))
                {
                    players = conn.Query<PlayerAndTeamData>(sqlDelete, new { position }).ToList().Count;
                }
                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
                return players;
            }

        }

        public int GetAllPlayers()
        {
            int players;

            var mp = MiniProfiler.StartNew("GetAllPlayers");

            using (var conn = OpenConnection())
            {
                var querySQL = @"SELECT * FROM licenta.players ORDER BY id ASC;";
                using (mp.Step("Execute query"))
                {
                    players = conn.Query<Player>(querySQL).ToList().Count;
                }
                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
                return players;
            }
        }

        public int GetTeamsByPoints(long points)
        {
            int teams;

            var mp = MiniProfiler.StartNew("GetTeamsByPoints");

            using (var conn = OpenConnection())
            {
                var querySQL = @"SELECT * FROM licenta.teams WHERE teams.points > @points;";

                using (mp.Step("Execute query"))
                {
                    teams = conn.Query<Team>(querySQL, new { points }).ToList().Count;
                }

                mp.Stop();
                Console.WriteLine(mp.RenderPlainText());
                return teams;
            }
        }

    }
}
