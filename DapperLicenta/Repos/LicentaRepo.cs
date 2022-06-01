using Dapper;
using DapperLicenta.Datas;
using DapperLicenta.Models;
using Npgsql;
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
            return conn;
        }

        public void InsertTeam(TeamData teamData)
        {
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
                conn.Execute(querySQL, teamData);
            }

        }

        public void UpdateTeam(TeamData teamData)
        {
            using (var conn = OpenConnection())
            {
                var sqlUpdate = "UPDATE licenta.teams SET points = @points WHERE name = @name";
                conn.Execute(sqlUpdate, new { teamData.Points, teamData.Name});
            }

        }

        public void DeletePlayer(string position, long age)
        {
            using (var conn = OpenConnection())
            {
                var sqlDelete = "DELETE FROM licenta.players WHERE position = @position AND age > @age";
                conn.Execute(sqlDelete, new {position, age});
            }
        }

        public int GetPlayerAndTeamByPosition(string position)
        {
            using (var conn = OpenConnection())
            {
                var sqlDelete = @"SELECT players.*, teams.country, teams.city, teams.points FROM licenta.players INNER JOIN licenta.teams ON players.team = teams.name
                       WHERE players.position = @position AND players.team LIKE '%United'"; 

                return conn.Query<PlayerAndTeamData>(sqlDelete, new {position}).ToList().Count;
            }

        }

        public int GetAllPlayers()
        {
            using (var conn = OpenConnection())
            {
                var querySQL = @"SELECT * FROM licenta.players ORDER BY id ASC;";
                return conn.Query<Player>(querySQL).ToList().Count;
            }
        }

        public int GetTeamsByPoints(long points)
        {
            using (var conn = OpenConnection())
            {
                var querySQL = @"SELECT * FROM licenta.teams WHERE teams.points > @points;";
                return conn.Query<Team>(querySQL, new {points}).ToList().Count;
            }
        }

    }
}
