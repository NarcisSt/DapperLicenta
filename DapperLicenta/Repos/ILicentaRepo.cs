using DapperLicenta.Datas;

namespace DapperLicenta.Repos
{
    public interface ILicentaRepo
    {
        void InsertTeam(TeamData teamData);
        void UpdateTeam(TeamData teamData);
        void DeletePlayer(string position, long age);
        int GetPlayerAndTeamByPosition(string position);
        int GetAllPlayers();
        int GetTeamsByPoints(long points);
    }
}
